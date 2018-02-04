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
	"time"

	"github.com/mindeng/goutils"

	"github.com/gorilla/websocket"
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
	taskMap map[int]*archiveTask
	lock    sync.RWMutex

	total   int32
	existed int32
	copied  int32
	failed  int32
}

func NewArchiver() *Archiver {
	return &Archiver{taskMap: make(map[int]*archiveTask)}
}

func (archiver *Archiver) archive(p string) {
	files := make(chan string, 100)
	tasks := make(chan *archiveTask, 100)
	// checkFileTasks := make(chan *archiveTask, 100)
	// copyTasks := make(chan *archiveTask, 100)

	go archiver.walkDirectory(p, files)
	go archiver.extractFilesTime(files, tasks)

	// go archiver.checkRemotePathExists(tasks, checkFileTasks, copyTasks)
	// go archiver.checkFileExists(checkFileTasks, copyTasks)
	// go archiver.copyFiles(copyTasks)

	do := func(wg *sync.WaitGroup) {
		for task := range tasks {
			archiver.doArchive(task)
		}
		wg.Done()
	}

	concurrency := 4
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go do(&wg)
	}

	wg.Wait()

	log.Print("total:", archiver.total)
	log.Print("existed:", archiver.existed)
	log.Print("copied:", archiver.copied)
	log.Print("failed:", archiver.failed)
	log.Print("unknown:", archiver.total-archiver.existed-archiver.copied-archiver.failed)
}

func (archiver *Archiver) getTask(id int) *archiveTask {
	archiver.lock.RLock()
	defer archiver.lock.RUnlock()
	return archiver.taskMap[id]
}

func (archiver *Archiver) putTask(id int, task *archiveTask) {
	archiver.lock.Lock()
	defer archiver.lock.Unlock()
	archiver.taskMap[id] = task
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
		archiver.putTask(id, &task)
		out <- &task
		id++
	}
	close(out)
}

func (archer *Archiver) sendReq(url *url.URL, req *mediakeeper.GeneralRequest, r io.Reader) (*mediakeeper.GeneralResponse, error) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
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

	if err := c.WriteJSON(&req); err != nil {
		return nil, err
	}
	if r != nil {
		w, err := c.NextWriter(websocket.BinaryMessage)
		if err != nil {
			return nil, err
		}

		_, err1 := io.Copy(w, r)
		err2 := w.Close()
		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}
	}

	var resp mediakeeper.GeneralResponse
	if err = c.ReadJSON(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (archiver *Archiver) copyToServer(task *archiveTask) (*mediakeeper.GeneralResponse, error) {
	// Check path exists
	u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/cp"}

	info, err := os.Stat(task.src)
	if err != nil {
		log.Fatal(task.src, err)
	}
	data, err := json.Marshal(mediakeeper.CopyData{ModTime: info.ModTime(), Md5: task.md5})
	if err != nil {
		log.Fatal(task.src, err)
	}

	req := mediakeeper.GeneralRequest{ID: task.id, Path: task.dst, Data: data}
	if f, err := os.Open(task.src); err != nil {
		log.Fatal(f.Name(), ":", err)
	} else {
		defer f.Close()
		if resp, err := archiver.sendReq(&u, &req, f); err != nil {
			return nil, err
		} else {
			return resp, nil
		}
	}
	return nil, err
}

func (archiver *Archiver) doArchive(task *archiveTask) {
	// Check path exists
	u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/pathExists"}
	req := mediakeeper.GeneralRequest{ID: task.id, Path: task.dst}
	resp, err := archiver.sendReq(&u, &req, nil)
	if err != nil {
		log.Println("check path exists:", err)
		atomic.AddInt32(&archiver.failed, 1)
		return
	}

	copy := func(task *archiveTask) {
		if resp, err := archiver.copyToServer(task); err != nil {
			log.Print("copy failed:", err)
			atomic.AddInt32(&archiver.failed, 1)
		} else {
			if resp.Ok {
				log.Print("copied: ", task.src, " ", task.dst)
				atomic.AddInt32(&archiver.copied, 1)
			} else {
				log.Print("copy failed:", err)
				atomic.AddInt32(&archiver.failed, 1)
			}
		}
	}

	if resp.Ok {
		// path exists, check file exists
		u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/fileExists"}

		if md5, err := goutils.FileMd5(task.src); err != nil {
			log.Fatal("md5:", err)
		} else {
			task.md5 = md5
		}
		req := mediakeeper.GeneralRequest{ID: task.id, Path: task.dst, Data: []byte(task.md5)}
		resp, err := archiver.sendReq(&u, &req, nil)
		if err != nil {
			log.Println("check file exists:", err)
			atomic.AddInt32(&archiver.failed, 1)
			return
		}

		if resp.Ok {
			log.Print("file exists:", task.src)
			atomic.AddInt32(&archiver.existed, 1)
		} else {
			copy(task)
		}
	} else {
		// copy
		copy(task)
	}
}

func (archer *Archiver) checkRemotePathExists(in <-chan *archiveTask, md5CheckTasks chan<- *archiveTask, copyTasks chan<- *archiveTask) {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/pathExists"}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer c.Close()
		defer close(done)
		for {
			var resp mediakeeper.GeneralResponse
			if err := c.ReadJSON(&resp); err != nil {
				log.Println("checkRemotePathExists read:", err)
				close(md5CheckTasks)
				return
			}
			task := archer.getTask(resp.ID)
			if resp.Ok {
				if md5, err := goutils.FileMd5(task.src); err != nil {
					log.Fatal("md5:", err)
				} else {
					task.md5 = md5
					md5CheckTasks <- task
				}
			} else {
				copyTasks <- task
			}
		}
	}()

	shutdown := func() {
		// To cleanly close a connection, a client should send a close
		// frame and wait for the server to close the connection.
		err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("checkRemotePathExists write close:", err)
			return
		}
		<-done
		c.Close()
	}

	for {
		select {
		case task, ok := <-in:
			if ok {
				req := mediakeeper.GeneralRequest{ID: task.id, Path: task.dst}
				if err := c.WriteJSON(&req); err != nil {
					log.Println("checkRemotePathExists write:", err)
					return
				}
			} else {
				log.Print("check path exists done.")
				shutdown()
				return
			}
		case <-interrupt:
			log.Println("checkRemotePathExists interrupt")
			shutdown()
			return
		}
	}
}

func (archer *Archiver) checkFileExists(in <-chan *archiveTask, copyTasks chan<- *archiveTask) {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/fileExists"}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})
	archer.existed = 0

	go func() {
		defer c.Close()
		defer close(done)
		for {
			var resp mediakeeper.GeneralResponse
			if err := c.ReadJSON(&resp); err != nil {
				log.Println("checkFileExists read:", err)
				return
			}
			task := archer.getTask(resp.ID)
			if resp.Ok {
				log.Print("file exists:", task.src)
				archer.existed++
			} else {
				if resp.Err != "" {
					log.Fatal(resp)
				}
				log.Print("cp task:", task)
				copyTasks <- task
			}
		}
	}()

	shudown := func() {
		// To cleanly close a connection, a client should send a close
		// frame and wait for the server to close the connection.
		err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("checkFileExists write close:", err)
			return
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		c.Close()
	}

	for {
		select {
		case task, ok := <-in:
			if ok {
				req := mediakeeper.GeneralRequest{ID: task.id, Path: task.dst, Data: []byte(task.md5)}
				if err := c.WriteJSON(&req); err != nil {
					log.Println("checkFileExists write:", err)
					return
				}
			} else {
				log.Print("check file exists tasks done.")
				shudown()
				return
			}
		case <-interrupt:
			log.Println("checkFileExists interrupt")
			shudown()
			return
		}
	}
}

func (archer *Archiver) copyFiles(in <-chan *archiveTask) {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *host + ":" + *port, Path: "/cp"}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})
	archer.copied = 0
	archer.failed = 0

	go func() {
		defer c.Close()
		defer close(done)
		for {
			var resp mediakeeper.GeneralResponse
			if err := c.ReadJSON(&resp); err != nil {
				log.Println("copyFiles read:", err)
				break
			}
			task := archer.getTask(resp.ID)
			if resp.Ok {
				archer.copied++
				log.Print("copied:", task.src, task.dst)
			} else {
				archer.failed++
				log.Println("copy failed:", task.src, resp.Err)
			}
		}

		log.Print("total:", archer.total)
		log.Print("existed:", archer.existed)
		log.Print("copied:", archer.copied)
		log.Print("failed:", archer.failed)
	}()

	shutdown := func() {
		// To cleanly close a connection, a client should send a close
		// frame and wait for the server to close the connection.
		err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("copyFiles write close:", err)
			return
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		c.Close()
	}

	for {
		select {
		case task, ok := <-in:
			if ok {
				info, err := os.Stat(task.src)
				if err != nil {
					log.Fatal(task.src, err)
				}
				data, err := json.Marshal(mediakeeper.CopyData{ModTime: info.ModTime(), Md5: task.md5})
				if err != nil {
					log.Fatal(task.src, err)
				}
				req := mediakeeper.GeneralRequest{ID: task.id, Path: task.dst, Data: data}
				if err := c.WriteJSON(&req); err != nil {
					log.Println("copyFiles write:", err)
					return
				}
				w, err := c.NextWriter(websocket.BinaryMessage)
				if err != nil {
					log.Println("copyFiles NextWriter:", err)
					return
				}
				if f, err := os.Open(task.src); err != nil {
					log.Fatal(f.Name(), ":", err)
				} else {
					io.Copy(w, f)
					f.Close()
				}
			} else {
				log.Print("copy tasks done.")
				shutdown()
				return
			}
		case <-interrupt:
			log.Println("copyFiles interrupt")
			shutdown()
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
