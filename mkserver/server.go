package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/mindeng/goutils"
	"github.com/mindeng/mediakeeper"

	"github.com/gorilla/websocket"
)

var host = flag.String("host", "", "host")
var port = flag.String("port", "3333", "port")

var upgrader = websocket.Upgrader{} // use default options

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

	http.HandleFunc("/pathExists", pathExistsHandler)
	http.HandleFunc("/fileExists", fileExistsHandler)
	http.HandleFunc("/cp", makeHandler(cpHandler))
	log.Fatal(http.ListenAndServe(*host+":"+*port, nil))
}
