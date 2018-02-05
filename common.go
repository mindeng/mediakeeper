package mediakeeper

import (
	"io"
	"os"
	"time"

	"github.com/cespare/xxhash"
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

func FileHash(p string) (uint64, error) {
	if f, err := os.Open(p); err != nil {
		return 0, err
	} else {
		defer f.Close()
		h := xxhash.New()
		if _, err := io.Copy(h, f); err != nil {
			return 0, err
		}
		return h.Sum64(), nil
	}
}

// func (resp *GeneralResponse) String() string {
// 	if s, err := json.Marshal(&resp); err != nil {
// 		log.Print(err)
// 		return ""
// 	} else {
// 		return string(s)
// 	}
// }
