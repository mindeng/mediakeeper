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
	"time"

	"github.com/mindeng/goutils"
	"github.com/mindeng/mediakeeper"

	"github.com/gorilla/websocket"
)

var host = flag.String("host", "", "host")
var port = flag.String("port", "3333", "port")

var upgrader = websocket.Upgrader{} // use default options

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
	if err := saveFile(p, data, r); err != nil {
		resp.Err = err.Error()
	} else {
		resp.Ret = mediakeeper.RetOK
		resp.Data = []byte(p)
	}
	return &resp
}

func CopyFileFromReader(dst string, src io.Reader, modTime time.Time, checksum uint64) error {
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	f.Close()

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

func saveFile(p string, info mediakeeper.FileInfo, r io.Reader) error {
	if err := os.MkdirAll(path.Dir(p), 0755); err != nil {
		return err
	}

	if err := CopyFileFromReader(p, r, info.ModTime, info.Hash); err != nil {
		return err
	}

	return nil
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
			resp = handleCmdGetHash(&req)
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

			resp = handleCmdPutFile(&req, r)
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

func makeHandler(handler func(*websocket.Conn) (interface{}, error)) func(http.ResponseWriter, *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()

		for {
			resp, err := handler(c)
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					log.Println(goutils.GetFuncName(handler), err)
				}
				break
			} else {
				// log.Print(resp)
				if err := c.WriteJSON(resp); err != nil {
					log.Println("write:", err)
					break
				}
			}
		}
	}
}

func pathExistsHandler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()
	for {
		var req mediakeeper.GeneralRequest
		if err := c.ReadJSON(&req); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				log.Println("pathExistsHandler read:", err)
			}
			break
		}

		p := req.Path
		p = path.Join(rootDir, p)
		resp := mediakeeper.GeneralResponse{Ok: false, ID: req.ID}
		if _, err := os.Stat(p); err == nil {
			resp.Ok = true
		}

		err = c.WriteJSON(resp)
		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

func fileExistsHandler(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()
	for {
		var req mediakeeper.GeneralRequest
		if err := c.ReadJSON(&req); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				log.Println("fileExistsHandler read:", err)
			}
			break
		}

		p := req.Path
		p = path.Join(rootDir, p)
		resp := mediakeeper.GeneralResponse{Ok: false, ID: req.ID}
		md5 := string(req.Data)
		if _, err := os.Stat(p); err == nil {
			if dstMd5, err := goutils.FileMd5(p); err != nil {
				resp.Err = err.Error()
			} else if dstMd5 == md5 {
				resp.Ok = true
			}
		}

		if resp.Ok != true {
			// check file with postfix
			ext := path.Ext(p)
			p2 := p[:len(p)-len(ext)] + "-" + md5[:6] + ext
			if _, err := os.Stat(p2); err == nil {
				resp.Ok = true
			}
		}

		err = c.WriteJSON(resp)
		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

func cpHandler(c *websocket.Conn) (interface{}, error) {
	resp := mediakeeper.GeneralResponse{Ok: false}
	var req mediakeeper.GeneralRequest

	if err := c.ReadJSON(&req); err != nil {
		return resp, err
	}

	resp.ID = req.ID
	p := path.Join(rootDir, req.Path)

	var data mediakeeper.CopyData
	err := json.Unmarshal(req.Data, &data)
	if err != nil {
		return resp, err
	}
	mt, r, err := c.NextReader()
	if err != nil {
		return resp, err
	}
	if mt != websocket.BinaryMessage {
		log.Fatal("cpHandler: message type is not binary")
	}

	if _, err := os.Stat(p); err == nil {
		if data.Md5 != "" {
			dstMd5, err := goutils.FileMd5(p)
			if err != nil {
				resp.Err = err.Error()
				return resp, nil
			}

			if data.Md5 == dstMd5 {
				err := errors.New("file duplicated")
				resp.Err = err.Error()
				return resp, nil
			}

			os.MkdirAll(path.Dir(p), 0755)
			ext := path.Ext(p)
			p = p[:len(p)-len(ext)] + "-" + data.Md5[:6] + ext

			log.Println("copy to", p)
			if err := goutils.CopyFileFromReader(p, r, data.ModTime); err != nil {
				resp.Err = err.Error()
				return resp, nil
			} else {
				resp.Ok = true
			}
		} else {
			err := errors.New("path exists")
			resp.Err = err.Error()
			return resp, nil
		}
	} else {
		log.Println("copy to", p)
		os.MkdirAll(path.Dir(p), 0755)
		if err := goutils.CopyFileFromReader(p, r, data.ModTime); err != nil {
			resp.Err = err.Error()
			return resp, nil
		} else {
			resp.Ok = true
			return resp, nil
		}
	}

	return resp, nil
}

var rootDir string

func main() {
	flag.Parse()
	rootDir = flag.Arg(0)
	log.Print("root:", rootDir)

	log.SetFlags(0)

	http.HandleFunc("/archive", archiveHandler)
	http.HandleFunc("/pathExists", pathExistsHandler)
	http.HandleFunc("/fileExists", fileExistsHandler)
	http.HandleFunc("/cp", makeHandler(cpHandler))
	log.Fatal(http.ListenAndServe(*host+":"+*port, nil))
}
