package mediakeeper

import (
	"time"
)

type CmdID int

const (
	CmdGetHash CmdID = iota
	CmdPutFile
)

type ReturnCode int

const (
	RetOK ReturnCode = iota
	RetFileNotExists
	RetFileExists
	RetError
)

type CmdRequest struct {
	Cmd  CmdID
	Path string
	Data []byte
}

type CmdResponse struct {
	Ret  ReturnCode
	Err  string
	Data []byte
}

type FileInfo struct {
	ModTime time.Time
	Size    int64
	Hash    uint64
}
