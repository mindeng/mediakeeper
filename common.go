package mediakeeper

import (
	"time"
)

type GeneralRequest struct {
	Path string
	ID   int
	Data []byte
}
type GeneralResponse struct {
	Ok  bool
	ID  int
	Err string
	// Data interface{}
}

type CopyData struct {
	ModTime time.Time
	Md5     string
}

// func (resp *GeneralResponse) String() string {
// 	if s, err := json.Marshal(&resp); err != nil {
// 		log.Print(err)
// 		return ""
// 	} else {
// 		return string(s)
// 	}
// }
