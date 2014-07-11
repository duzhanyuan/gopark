package moosefs

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
)

var MFS_PREFIX = make(map[string]string)

var ENABLE_DCACHE = false

var MFS_ROOT_INODE uint32 = 1

var _mfs = make(map[string]*MooseFS)

type MooseFS struct {
	host          string
	port          int32
	mountpoint    string
	mc            *MasterConn
	symlink_cache map[uint32]string
}

//host:mfsmaster port:9421 mountpoint:/mfs
func NewMooseFS(host string, port int32, mountpoint string) *MooseFS {
	mc := NewMasterConn(host, port)
	m := &MooseFS{
		host:          host,
		port:          port,
		mountpoint:    mountpoint,
		mc:            mc,
		symlink_cache: make(map[uint32]string),
	}
	return m
}

func (m *MooseFS) Close() {
	m.mc.Close()
}

func (m *MooseFS) _lookup(parent uint32, name string) (*FileInfo, error) {
	return m.mc.Lookup(parent, name)
}

func (m *MooseFS) readlink(inode uint32) (string, error) {
	var target string
	var err error
	target, _ = m.symlink_cache[inode]
	if target == "" {
		target, err = m.mc.ReadLink(inode)
		if err != nil {
			return target, err
		}
		m.symlink_cache[inode] = target
	}
	return target, nil
}

//path:string, followSymlink:true
func (m *MooseFS) Lookup(fpath string, followSymlink bool) (*FileInfo, error) {
	parent := MFS_ROOT_INODE
	var info *FileInfo
	var err error
	ps := strings.Split(fpath, "/")
	for idx, n := range ps {
		if n != "" {
			if info, err = m._lookup(parent, n); err != nil {
				return nil, err
			} else {
				for info.IsSymlink() && followSymlink {
					var target string
					target, err = m.readlink(info.Inode)
					if !(strings.Index(target, "/") == 0) {
						target = path.Join(strings.Join(ps[:idx], "/"), target)
						info, err = m.Lookup(target, followSymlink)
					} else if strings.Index(target, m.mountpoint) == 0 {
						info, err = m.Lookup(target[len(m.mountpoint):], followSymlink)
					} else {
						src := fpath
						dst := path.Join(target, strings.Join(ps[idx+1:], "/"))
						return nil, fmt.Errorf("%s->%s", src, dst)
					}
				}
			}
			parent = info.Inode
		}
	}
	if info == nil && parent == MFS_ROOT_INODE {
		info, err = m.mc.getattr(parent)
	}
	return info, err
}

func (m *MooseFS) Open(fpath string) (*File, error) {
	if info, err := m.Lookup(fpath, true); err != nil {
		return nil, err
	} else {
		if info == nil {
			return nil, errors.New("not found")
		}
		return NewFile(info.Inode, fpath, info, m.host), nil
	}
}

func (m *MooseFS) ListDir(fpath string) (map[string]*FileInfo, error) {
	if info, err := m.Lookup(fpath, true); err != nil {
		return nil, err
	} else {
		if info == nil {
			return nil, errors.New("not found")
		}
		return m.mc.GetDirPlus(info.Inode)
	}
}

type WalkItem struct {
	Root  string
	Dirs  []string
	Files []string
}

func MapGetKeys(m map[string]interface{}) []string {
	if len(m) > 0 {
		vals := make([]string, len(m))
		idx := 0
		for k, _ := range m {
			vals[idx] = k
			idx += 1
		}
		return vals
	}
	return nil
}

//path string, followlinks: false
//return : <-chan WalkItem | error
func (m *MooseFS) Walk(done chan bool, fpath string, followlinks bool) <-chan interface{} {
	ch := make(chan interface{}) //WalkItem
	go func() {
		defer close(ch)
		ds := []string{fpath}
		for len(ds) > 0 {
			root := ds[0]
			ds = ds[1:]
			dirs := make(map[string]interface{})
			files := make(map[string]interface{})
			if cs, err := m.ListDir(root); err != nil {
				ChanError(done, ch, err)
				return
			} else {
				if len(cs) > 0 {
					for name, info := range cs {
						if name == "." || name == ".." {
							continue
						}
						for followlinks && info != nil && info.Type == TYPE_SYMLINK {
							if target, err := m.readlink(info.Inode); err != nil {
								ChanError(done, ch, err)
								return
							} else {
								if strings.HasPrefix(target, "/") {
									if !strings.HasPrefix(target, m.mountpoint) {
										if fi, ok := isFileDirExists(target); ok {
											if fi.IsDir() {
												dirs[target] = true
											} else {
												files[target] = true
											}
										}
										info = nil //TODO: is something wrong?
										break
									} else {
										target = target[len(m.mountpoint):]
										//DONE:# use relative path for internal symlinks
										st := strings.Split(strings.Trim(root, "/"), "/")
										name = path.Join(strings.Repeat("../", len(st)), target)
									}
								} else {
									name = target
									target = path.Join(root, target)
								}
								var err2 error
								info, err2 = m.Lookup(target, false)
								if err2 != nil {
									ChanError(done, ch, err2)
									return
								}
							}
						}
						if info != nil {
							if info.Type == TYPE_DIRECTORY {
								if _, ok := dirs[name]; !ok {
									dirs[name] = true
								}
							} else if info.Type == TYPE_FILE {
								if _, ok := files[name]; !ok {
									files[name] = true
								}
							}
						}

					}
					item := WalkItem{
						Root:  root,
						Dirs:  MapGetKeys(dirs),
						Files: MapGetKeys(files),
					}
					select {
					case ch <- item:
					case <-done:
						return
					}
					for d, _ := range dirs {
						if !strings.HasPrefix(d, "/") { // skip external links
							ds = append(ds, path.Join(root, d))
						}
					}
				}
			}
		}
	}()
	return ch
}

type File struct {
	Inode   uint32
	Path    string
	Info    *FileInfo
	Length  uint64
	Master  string
	cscache map[uint32]*Chunk
}

func NewFile(inode uint32, fpath string, info *FileInfo, master string) *File {
	return &File{
		Inode:  inode,
		Path:   fpath,
		Info:   info,
		Length: info.Length,
		Master: master,
	}
}

func (f *File) Get_Chunk(i uint32) (*Chunk, error) {
	chunk, _ := f.cscache[i]
	if chunk == nil {
		if chunk, err := GetMFS(f.Master).mc.ReadChunk(f.Inode, i); err != nil {
			f.cscache[i] = chunk
			return chunk, nil
		} else {
			return nil, err
		}
	}
	return chunk, nil
}

func (f *File) LocsAll() ([][]string, error) {
	n := f.Length - 1/CHUNKSIZE + 1
	if n <= 0 {
		return nil, nil
	}
	results := make([][]string, n)
	var i uint32
	for i = 0; i < uint32(n); i++ {
		if chunk, err := f.Get_Chunk(i); err != nil {
			return nil, err
		} else {
			results[i] = chunk.Addrs
		}
	}
	return results, nil
}

func (f *File) Locs(i uint32) ([]string, error) {
	if chunk, err := f.Get_Chunk(i); err != nil {
		return nil, err
	} else {
		return chunk.Addrs, nil
	}
}

// Readablefile

type ReadableFile struct {
	*File
	roff     uint64
	rbuf     []byte
	reader   <-chan interface{}
	readerCh chan bool
}

func NewReadableFile(file *File) *ReadableFile {
	return &ReadableFile{
		File: file,
		roff: 0,
	}
}

func (r *ReadableFile) Name() string {
	return r.File.Info.Name
}

func (r *ReadableFile) seek(offset int64, whence uint64) {
	if whence == 1 {
		offset = int64(r.roff) + offset
	} else if whence == 2 {
		offset = int64(r.Length) + offset
	}
	if !(offset >= 0) {
		panic("offset should greater than 0")
	}

	off := offset - int64(r.roff)
	if off > 0 && off < int64(len(r.rbuf)) {
		r.rbuf = r.rbuf[off:]
	} else {
		r.rbuf = nil
		r.reader = nil
	}
	r.roff = uint64(offset)
}

func (r *ReadableFile) tell() uint64 {
	return r.roff
}

func (r *ReadableFile) read(n uint64) []byte {
	if n == 0 {
		if r.rbuf == nil {
			r.fill_buffer()
		}
		v := r.rbuf
		r.roff += uint64(len(v))
		r.rbuf = nil
		return v
	}

	buf := new(bytes.Buffer)
	for n > 0 {
		nbuf := uint64(len(r.rbuf))
		if nbuf >= n {
			buf.Write(r.rbuf[:n])
			r.rbuf = r.rbuf[n:]
			r.roff += n
			break

		}
		if nbuf > 0 {
			buf.Write(r.rbuf)
			n -= nbuf
			r.rbuf = nil
			r.roff += nbuf
		}
		r.fill_buffer()
		if r.rbuf == nil {
			break
		}
	}
	return buf.Bytes()
}

func (r *ReadableFile) Seek(offset int64, whence int) (int64, error) {
	/*
		if !(offset >= 0) {
			return 0, errors.New("offset should greater than 0")
		}
	*/
	if whence == 1 {
		offset = int64(r.roff) + offset
	} else if whence == 2 {
		offset = int64(r.Length) + offset
	}

	r.roff = uint64(offset)
	off := offset - int64(r.roff)
	if off > 0 && off < int64(len(r.rbuf)) {
		r.rbuf = r.rbuf[off:]
	} else {
		r.rbuf = nil
		r.reader = nil
	}
	return int64(r.roff), nil
}

func (r *ReadableFile) Read(buffs []byte) (int, error) {
	n := cap(buffs)
	if n == 0 {
		return 0, nil
	}

	buf := new(bytes.Buffer)
	for n > 0 {
		nbuf := len(r.rbuf)
		if nbuf >= n {
			buf.Write(r.rbuf[:n])
			r.rbuf = r.rbuf[n:]
			r.roff += uint64(n)
			break

		}
		if nbuf > 0 {
			buf.Write(r.rbuf)
			n -= nbuf
			r.rbuf = nil
			r.roff += uint64(nbuf)
		}
		r.fill_buffer()
		if r.rbuf == nil {
			break
		}
	}
	buffs = buf.Bytes()[:n]
	return n, nil
}

func (r *ReadableFile) fill_buffer() error {
	if r.reader == nil {
		r.readerCh = make(chan bool)
		if r.roff < r.Length {
			if reader, err := r.chunk_reader(r.readerCh, r.roff); err != nil {
				return err
			} else {
				r.reader = reader
			}
		} else {
			return nil
		}
	}

	if rbuf, ok := <-r.reader; ok {
		if buf, ok := rbuf.([]byte); ok {
			r.rbuf = buf
		} else {
			return rbuf.(error)
		}
	} else {
		//reader stop
		r.reader = nil
		if r.readerCh != nil {
			close(r.readerCh)
			r.readerCh = nil
		}
		r.fill_buffer()
	}
	return nil
}

func (r ReadableFile) isLocal(chunk *Chunk) bool {
	name, err := os.Hostname()
	if err != nil {
		return false
	}
	addrs, err := net.LookupHost(name)
	if err != nil {
		return false
	}
	if chunk != nil && chunk.Addrs != nil {
		for _, addr := range chunk.Addrs {
			for _, host := range addrs {
				if strings.HasPrefix(addr, host+":") {
					return true
				}
			}
		}
	}
	return false
}

// <-chan  []byte | error
func (r *ReadableFile) chunk_reader(done chan bool, roff uint64) (<-chan interface{}, error) {
	index := roff / CHUNKSIZE
	offset := roff % CHUNKSIZE
	var length uint64
	if chunk, err := r.Get_Chunk(uint32(index)); err != nil {
		return nil, err
	} else {
		length = MinUint64(r.Length-uint64(index*CHUNKSIZE), CHUNKSIZE)
		if offset > length {
			return nil, nil
		}
		if r.isLocal(chunk) {
			blocks, err := read_chunk_from_local(done, chunk.Id, chunk.Version, length-offset, offset)
			if err != nil {
				return nil, err
			}
			return blocks, nil
		} else {
			//remote
			ch := make(chan interface{})
			go func() {
				close(ch)
				for _, remote := range chunk.Addrs {
					remotes := strings.Split(remote, ":")
					host := remotes[0]
					port, _ := strconv.Atoi(remotes[1])
					var blocks <-chan interface{}
					var err error
					//DONE: give up after two continuous errors
					nerror := 0
				ReadBlocks:
					blocks, err = read_chunk(done, host, uint32(port),
						chunk.Id, chunk.Version, length-offset, offset)
					if err != nil {
						nerror += 1
						if nerror < 2 {
							goto ReadBlocks
						}
						ChanError(done, ch, err)
						return
					} else {
						for block := range blocks {
							if buf, ok := block.([]byte); ok {
								select {
								case ch <- buf:
								case <-done:
									return
								}
							} else {
								select {
								case ch <- block:
									return
								case <-done:
									return
								}
							}
						}
					}
				}
			}()
			return ch, nil
		}
	}
}

// <- chan string | error, error
func (r *ReadableFile) AllLines(done chan bool) <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		buf := new(bytes.Buffer)
		for {
			data := r.read(0)
			if len(data) == 0 {
				return
			}
			buf.Write(data)
			datas := buf.Bytes()
			idx := bytes.IndexByte(datas, '\n')
			if idx > 0 {
				result := string(datas[:idx])
				buf = bytes.NewBuffer(datas[idx:])
				select {
				case ch <- result:
				case <-done:
					return
				}

			} else {
				buf.Write(data)
			}
		}
	}()
	return ch
}

func (r *ReadableFile) Close() error {
	r.roff = 0
	r.rbuf = nil
	r.reader = nil
	return nil
}

func MFSOpen(path string, master string) (*File, error) {
	return GetMFS(master).Open(path)
}

func GetMFS(master string) *MooseFS {
	return GetMFSMount(master, "/mfs")
}
func GetMFSMount(master string, mountpoint string) *MooseFS {
	if m, ok := _mfs[master]; ok {
		return m
	}
	m := NewMooseFS(master, 9421, mountpoint)
	_mfs[master] = m
	return m
}

// master = "mfsmaster"
func ListDir(path, master string) (map[string]*FileInfo, error) {
	return GetMFS(master).ListDir(path)
}

func GetMFSByPath(path string) *MooseFS {
	for prefix, master := range MFS_PREFIX {
		if strings.HasPrefix(path, prefix) {
			return GetMFSMount(master, prefix)
		}
	}
	return nil
}

func add_prefix(done chan bool, ch <-chan interface{}, prefix string) <-chan interface{} {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for n := range ch {
			var result interface{}
			if item, ok := n.(WalkItem); ok {
				item.Root = prefix + item.Root
				result = item
				for _, a := range item.Dirs {
					if strings.HasPrefix(a, "/") {
						wk := Walk(done, a, true)
						for w := range wk {
							select {
							case c <- w:
							case <-done:
								return
							}

						}
					}
				}
			} else {
				result = n
			}
			select {
			case c <- result:
			case <-done:
				return
			}
		}
	}()
	return c
}

func getRealPath(fpath string) string {
	_, filename, _, _ := runtime.Caller(1)
	return path.Join(path.Dir(filename), fpath)
}

//followlinks = false
func Walk(done chan bool, fpath string, followlinks bool) <-chan interface{} {
	fpath = getRealPath(fpath)
	mfs := GetMFSByPath(fpath)
	if mfs != nil {
		ch := mfs.Walk(done, fpath[len(mfs.mountpoint):], followlinks)
		return add_prefix(done, ch, mfs.mountpoint)
	} else {
		//return os.walk(path, followlinks)
		ch := make(chan interface{})
		go func() {
			defer close(ch)
			ChanError(done, ch, errors.New("mfs not exists."))
			return
		}()
		return ch
	}
}

func OpenFile(path string) (*File, error) {
	mfs := GetMFSByPath(path)
	if mfs != nil {
		return mfs.Open(path[len(mfs.mountpoint):])
	}
	//TODO:except CrossSystemSymlink, e:
	//   return open_file(e.dst)
	return nil, errors.New("mfs not exists.")
}
