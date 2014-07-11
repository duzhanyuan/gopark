package gopark

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"html"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
)

func getExeFileData() (string, string, []byte) {
	fname, fpath, err := getFilePath()
	if err != nil {
		glog.Fatal(err)
	}
	url := fmt.Sprintf("/%s.tar.gz", fname)
	if buff, err := _getFileData(fpath); err != nil {
		glog.Fatal(err)
	} else {
		return fname, url, buff.Bytes()
	}
	return "", "", nil
}

func _getFileData(fpath string) (*bytes.Buffer, error) {
	if data, err := ioutil.ReadFile(fpath); err != nil {
		return nil, err
	} else {
		if fi, err := os.Stat(fpath); err != nil {
			return nil, err
		} else {
			reads := bytes.NewReader(data)
			var buff bytes.Buffer
			gw := gzip.NewWriter(&buff)
			defer gw.Close()
			tw := tar.NewWriter(gw)
			defer tw.Close()
			hdr, err := tar.FileInfoHeader(fi, "")
			if err != nil {
				return nil, err
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return nil, err
			}
			if _, err := io.Copy(tw, reads); err != nil {
				return nil, err
			}
			return &buff, nil
		}
	}
}

// http serve for file get
type fileHandler struct {
	URL      string
	Data     []byte
	DataSize int
}

func newFileHandler(url string, data []byte) *fileHandler {
	return &fileHandler{
		URL:      url,
		Data:     data,
		DataSize: len(data),
	}
}

func (h *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	url := html.EscapeString(r.URL.Path)
	if url == h.URL {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(h.Data)))
		w.Write(h.Data)
		glog.Infof("Executor fetch %s[size:%d]", url, h.DataSize)
	}
}

func HttpServerForExecutor(url string, data []byte) string {
	if tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0"); err == nil {
		if lis, err := net.ListenTCP("tcp", tcpAddr); err == nil {
			addr := lis.Addr()
			if tcpAddr, ok := addr.(*net.TCPAddr); ok {
				port := tcpAddr.Port
				host, _ := os.Hostname()
				http_addr := fmt.Sprintf("http://%s:%d", host, port)
				glog.Infof("Executor fetch server: %s", http_addr)
				go func() {
					if err := http.Serve(lis, newFileHandler(url, data)); err != nil {
						glog.Fatal(err)
					}
				}()
				return fmt.Sprintf("%s%s", http_addr, url)
			}
		} else {
			glog.Fatal(err)
		}
	} else {
		glog.Fatal(err)
	}
	return ""
}

func getFilePath() (string, string, error) {
	fname := os.Args[0]
	f, err := filepath.Abs(fname)
	if err != nil {
		return "", "", err
	}
	return filepath.Base(fname), f, nil
}

func init() {
	gob.Register(&SimpleJobResult{})
	gob.Register(&SimpleJobTask{})
}

type SimpleJobResult struct {
	Err     error
	Result  interface{}
	Updates map[uint64]interface{}
}

type SimpleJobTask struct {
	Task _Tasker
}
