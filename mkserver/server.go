package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"bitbucket.org/minotes/mediakeeper"
	"github.com/gorilla/websocket"
)

var host = flag.String("host", "", "host")
var port = flag.String("port", "3333", "port")

var upgrader = websocket.Upgrader{} // use default options

type taskInfo struct {
	dst  string
	lock sync.RWMutex
}

type taskManager struct {
	tasks map[string]*taskInfo
	lock  sync.Mutex
}

var taskMgr = taskManager{tasks: make(map[string]*taskInfo)}

func handleCmdGetHash(req *mediakeeper.CmdRequest) *mediakeeper.CmdResponse {
	p := req.Path
	p = path.Join(rootDir, p)
	resp := mediakeeper.CmdResponse{Ret: mediakeeper.RetError}
	if info, err := os.Stat(p); err != nil {
		resp.Ret = mediakeeper.RetFileNotExists
	} else {
		h, err := mediakeeper.FileHash(p)
		if err != nil {
			resp.Err = err.Error()
			return &resp
		}
		if resp.Data, err = json.Marshal(
			mediakeeper.FileInfo{Size: info.Size(), Hash: h}); err != nil {
			resp.Err = err.Error()
		} else {
			resp.Ret = mediakeeper.RetOK
		}

	}

	return &resp
}
func handleCmdPutFile(req *mediakeeper.CmdRequest, r io.Reader) *mediakeeper.CmdResponse {
	resp := mediakeeper.CmdResponse{Ret: mediakeeper.RetError}

	p := path.Join(rootDir, req.Path)
	var data mediakeeper.FileInfo
	if err := json.Unmarshal(req.Data, &data); err != nil {
		resp.Err = err.Error()
		return &resp
	}

	if info, err := os.Stat(p); err == nil {
		dstHash, err := mediakeeper.FileHash(p)
		if err != nil {
			resp.Err = err.Error()
			return &resp
		}

		if data.Hash == dstHash && data.Size == info.Size() {
			resp.Ret = mediakeeper.RetFileExists
			return &resp
		}

		ext := path.Ext(p)
		p = fmt.Sprintf("%s-xxh-%x%s", p[:len(p)-len(ext)], data.Hash, ext)

		if info, err := os.Stat(p); err == nil && info.Size() == data.Size {
			// the file has been saved as the filename with the hash postfix
			resp.Ret = mediakeeper.RetFileExists
			resp.Err = p
			return &resp
		}
	}

	log.Println("save file ", p)
	if err := saveFile(p, data, r, true); err != nil {
		resp.Err = err.Error()
	} else {
		resp.Ret = mediakeeper.RetOK
		resp.Data = []byte(p)
	}
	return &resp
}

func handleCmdPutFileForce(req *mediakeeper.CmdRequest, r io.Reader) *mediakeeper.CmdResponse {
	resp := mediakeeper.CmdResponse{Ret: mediakeeper.RetError}

	p := path.Join(rootDir, req.Path)
	var data mediakeeper.FileInfo
	if err := json.Unmarshal(req.Data, &data); err != nil {
		resp.Err = err.Error()
		return &resp
	}

	log.Println("save file ", p)
	if err := saveFile(p, data, r, false); err != nil {
		resp.Err = err.Error()
	} else {
		resp.Ret = mediakeeper.RetOK
		resp.Data = []byte(p)
	}
	return &resp
}

func CopyFileFromReader(dst string, src io.Reader, modTime time.Time, checksum uint64, excl bool) error {
	if excl {
		f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
		f.Close()
	}

	in := src

	// tmp, err := ioutil.TempFile("", "")
	tmp, err := ioutil.TempFile(filepath.Dir(dst), "_tmp_")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	_, err = io.Copy(tmp, in)
	if err = tmp.Close(); err != nil {
		return err
	}

	h, err := mediakeeper.FileHash(tmp.Name())
	if err != nil {
		return err
	}
	if h != checksum {
		return errors.New("save file error: inconsistency")
	}

	if err = os.Chtimes(tmp.Name(), time.Now(), modTime); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	return os.Rename(tmp.Name(), dst)
}

func saveFile(p string, info mediakeeper.FileInfo, r io.Reader, excl bool) error {
	if err := os.MkdirAll(path.Dir(p), 0755); err != nil {
		return err
	}

	if err := CopyFileFromReader(p, r, info.ModTime, info.Hash, excl); err != nil {
		return err
	}

	return nil
}

func lockFile(p string, wflag bool, logid interface{}) *taskInfo {
	taskMgr.lock.Lock()
	if task, ok := taskMgr.tasks[p]; ok {
		taskMgr.lock.Unlock()

		log.Printf("[%p] Try to lock path %s", logid, p)
		if wflag {
			task.lock.Lock()
		} else {
			task.lock.RLock()
		}
		log.Printf("[%p] Locked path %s", logid, p)

		return task
	} else {
		// log.Printf("[%p] new task %s", logid, p)

		task = &taskInfo{dst: p}
		taskMgr.tasks[p] = task
		if wflag {
			task.lock.Lock()
		} else {
			task.lock.RLock()
		}
		taskMgr.lock.Unlock()
		return task
	}
}

func unlockFile(task *taskInfo, wflag bool, logid interface{}) {
	// log.Printf("[%p] del task %s", logid, task.dst)

	taskMgr.lock.Lock()
	delete(taskMgr.tasks, task.dst)
	taskMgr.lock.Unlock()

	if wflag {
		task.lock.Unlock()
	} else {
		task.lock.RUnlock()
	}
}

func archiveHandler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()

loop:
	for {
		var req mediakeeper.CmdRequest
		if err := c.ReadJSON(&req); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				log.Println("cmdHandler read:", err)
			}
			break
		}

		var resp *mediakeeper.CmdResponse
		switch req.Cmd {
		case mediakeeper.CmdGetHash:
			task := lockFile(req.Path, false, c)
			resp = handleCmdGetHash(&req)
			unlockFile(task, false, c)
		case mediakeeper.CmdPutFile:
			mt, r, err := c.NextReader()
			if err != nil {
				log.Println("error cmdHandler read:", err)
				break loop
			}
			if mt != websocket.BinaryMessage {
				log.Print("error CmdPutFile: message type is not binary")
				break loop
			}

			task := lockFile(req.Path, true, c)
			resp = handleCmdPutFile(&req, r)
			unlockFile(task, true, c)
		case mediakeeper.CmdPutFileForce:
			mt, r, err := c.NextReader()
			if err != nil {
				log.Println("error CmdPutFileForce read:", err)
				break loop
			}
			if mt != websocket.BinaryMessage {
				log.Print("error CmdPutFileForce: message type is not binary")
				break loop
			}

			task := lockFile(req.Path, true, c)
			resp = handleCmdPutFileForce(&req, r)
			unlockFile(task, true, c)
		default:
			log.Println("error cmd:", req.Cmd)
			break loop
		}

		err = c.WriteJSON(resp)
		if err != nil {
			log.Println("error cmdHandler write:", err)
			break
		}
	}
}

var rootDir string

func main() {
	flag.Parse()
	rootDir = flag.Arg(0)
	log.Print("root:", rootDir)

	http.HandleFunc("/archive", archiveHandler)
	log.Fatal(http.ListenAndServe(*host+":"+*port, nil))
}
