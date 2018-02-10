package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/mindeng/goutils"
	"github.com/mindeng/mediakeeper"
)

var host = flag.String("host", "localhost", "host")
var port = flag.String("port", "3333", "port")

type archiveTask struct {
	id  int
	src string
	dst string
	md5 string
}

type Archiver struct {
	total   int32
	existed int32
	copied  int32
	failed  int32
}

func NewArchiver() *Archiver {
	return &Archiver{}
}

func (archiver *Archiver) archive(p string) {
	files := make(chan string, 100)
	tasks := make(chan *archiveTask, 100)

	go archiver.walkDirectory(p, files)
	go archiver.extractFilesTime(files, tasks)

	do := func(tasks <-chan *archiveTask, wg *sync.WaitGroup) {
		archiver.archiveFiles(tasks)
		wg.Done()
	}

	concurrency := 4
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go do(tasks, &wg)
	}

	wg.Wait()

	log.Print("total:", archiver.total)
	log.Print("existed:", archiver.existed)
	log.Print("copied:", archiver.copied)
	log.Print("failed:", archiver.failed)
	log.Print("unknown:", archiver.total-archiver.existed-archiver.copied-archiver.failed)
}

func (archiver *Archiver) walkDirectory(dir string, tasks chan string) {
	archiver.total = 0
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "walk error: %v %s\n", err, path)
			return nil
		}
		// Ignore directory
		if info.IsDir() {
			return nil
		}
		// Ignore invalid files
		if info.Name()[0] == '.' && info.Size() == 4096 {
			return nil
		}
		switch strings.ToLower(filepath.Ext(info.Name())) {
		case ".jpg", ".jpeg", ".png", ".arw", ".nef", ".avi", ".mp4", ".mov", ".m4v", ".m4a":
			// need to archive
			// log.Println("put ", path)
			tasks <- path
			archiver.total++
			return nil
		default:
			return nil
		}
	})

	close(tasks)
}

func (archiver *Archiver) extractFilesTime(in <-chan string, out chan<- *archiveTask) {
	var id int = 0
	for p := range in {
		t, err := goutils.FileTime(p)
		if err != nil {
			log.Fatal(err)
		}

		dst := path.Join(fmt.Sprintf("%02d/%02d/%02d", t.Year(), t.Month(), t.Day()), path.Base(p))
		task := archiveTask{src: p, dst: dst, id: id}
		out <- &task
		id++
	}
	close(out)
}

func (archiver *Archiver) archiveFiles(tasks <-chan *archiveTask) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/archive"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	defer func() {
		err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("write close:", err)
			return
		}

		c.Close()
	}()

loop:
	for task := range tasks {

		// Get local file hash
		fileInfo, err := os.Stat(task.src)
		if err != nil {
			log.Println("error: ", err)
			atomic.AddInt32(&archiver.failed, 1)
			continue
		}

		// Get remote file hash
		req := mediakeeper.CmdRequest{Cmd: mediakeeper.CmdGetHash, Path: task.dst}
		var resp mediakeeper.CmdResponse

		if err := c.WriteJSON(&req); err != nil {
			log.Print("error:", err)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		if err = c.ReadJSON(&resp); err != nil {
			log.Print("error:", err)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		fileHash, err := mediakeeper.FileHash(task.src)
		if err != nil {
			atomic.AddInt32(&archiver.failed, 1)
			log.Println("error: ", err)
			continue
		}

		switch resp.Ret {
		case mediakeeper.RetFileNotExists:
			// go on to archive
		case mediakeeper.RetError:
			log.Println(resp.Err)
			// go on to archive
		case mediakeeper.RetOK:
			var data mediakeeper.FileInfo
			if err := json.Unmarshal(resp.Data, &data); err != nil {
				log.Println("error: ", err)
				atomic.AddInt32(&archiver.failed, 1)
				continue loop
			}
			if fileHash == data.Hash && fileInfo.Size() == data.Size {
				log.Println("existed: ", task.src)
				atomic.AddInt32(&archiver.existed, 1)
				continue loop
			}
		default:
			atomic.AddInt32(&archiver.failed, 1)
			log.Println("Invalid response:", resp)
			return
		}

		// Copy file to remote
		req.Cmd = mediakeeper.CmdPutFile
		req.Data, err = json.Marshal(mediakeeper.FileInfo{Hash: fileHash, Size: fileInfo.Size()})
		if err := c.WriteJSON(&req); err != nil {
			log.Print("error:", err)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		w, err := c.NextWriter(websocket.BinaryMessage)
		if err != nil {
			log.Print("error:", err)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		f, err := os.Open(task.src)
		if err != nil {
			log.Print("error:", err)
			atomic.AddInt32(&archiver.failed, 1)
			continue
		}

		_, err1 := io.Copy(w, f)
		err2 := w.Close()
		err3 := f.Close()
		if err1 != nil {
			log.Print("error:", err1)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}
		if err2 != nil {
			log.Print("error:", err2)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}
		if err3 != nil {
			log.Print("error:", err3)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		if err = c.ReadJSON(&resp); err != nil {
			log.Print("error:", err)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		switch resp.Ret {
		case mediakeeper.RetError:
			atomic.AddInt32(&archiver.failed, 1)
			log.Println(resp.Err)
			continue loop
		case mediakeeper.RetFileExists:
			log.Println("existed: ", task.src)
			atomic.AddInt32(&archiver.existed, 1)
			continue loop
		case mediakeeper.RetOK:
			atomic.AddInt32(&archiver.copied, 1)
			log.Println("archived:", task.src, string(resp.Data))
		default:
			atomic.AddInt32(&archiver.failed, 1)
			log.Println("Invalid response:", resp)
			return
		}
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	p := flag.Arg(0)
	if p == "" {
		log.Fatal("Usage: ", os.Args[0], " PATH")
	}

	archiver := NewArchiver()
	archiver.archive(p)
}
