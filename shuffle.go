package gopark

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"sync"
)

var MAX_SHUFFLE_MEMORY = 2000 //2GB

var LocalFileShuffle = _LocalFileShuffle{}

type _LocalFileShuffle struct {
	serverUri  string
	shuffleDir []string
}

func (s *_LocalFileShuffle) initialize(isMaster bool) {
	_workdirs := _env.Get("WORKDIR", nil)
	if _workdirs == nil {
		return
	} else {
		workdirs := _workdirs.([]string)
		shuffledirs := make([]string, len(workdirs))
		idx := 0
		for _, d := range workdirs {
			dpath := d
			if _, ok := ExistPath(dpath); ok {
				shuffledirs[idx] = dpath
				idx = idx + 1
			}
		}
		s.shuffleDir = shuffledirs[:idx]
		if idx == 0 {
			return
		}
		s.serverUri = _env.Get("SERVER_URI", "file://"+s.shuffleDir[0]).(string)
		glog.Infof("shuffle dir: %v", s.shuffleDir)
	}
}

func (s *_LocalFileShuffle) getOutputFile(shuffleId, inputId, outputId, datasize uint64) string {
	fpath := path.Join(s.shuffleDir[0], fmt.Sprintf("%d", shuffleId), fmt.Sprintf("%d", inputId))
	if _, ok := ExistPath(fpath); !ok {
		if err := os.MkdirAll(fpath, 0777); err != nil {
			glog.Fatal(err)
		}
	}
	p := path.Join(fpath, fmt.Sprintf("%d", outputId))
	dirLen := len(s.shuffleDir)
	if datasize > 0 && dirLen > 1 {
		free, total := getPathFreeTotal(fpath)
		ratio := float64(free) / float64(total)
		if uint64(free) < MaxUint64(datasize, 1<<30) || ratio < 0.66 {
			r := rand.Intn(dirLen-1) + 1
			d2 := path.Join(s.shuffleDir[r], fmt.Sprintf("%d", shuffleId), fmt.Sprintf("%d", inputId))
			if _, ok := ExistPath(d2); !ok {
				if err := os.MkdirAll(d2, 0777); err != nil {
					glog.Fatal(err)
				}
			}
			p2 := path.Join(d2, fmt.Sprintf("%d", outputId))
			if err := os.Symlink(p2, p); err != nil {
				glog.Fatal(err)
			} else {
				if fi, err := os.Lstat(p2); err != nil {
					glog.Fatal(err)
				} else {
					if fi.Mode() == os.ModeSymlink {
						if err := os.Remove(p2); err != nil {
							glog.Fatal(err)
						}
					}
				}
			}
			return p2
		}
	}
	return p
}

func (s *_LocalFileShuffle) getServerUri() string {
	return s.serverUri
}

type ShuffleFetcher interface {
	Fetch(shuffleId uint64, reduceId uint64, merge func(<-chan interface{}))
	Stop()
}

type ParallelShuffleFetcher struct {
	nthreads int
	requests chan *_FetcherRequest
}

func NewParallelShuffleFetcher(nthreads int) ShuffleFetcher {
	p := &ParallelShuffleFetcher{
		nthreads: nthreads,
		requests: make(chan *_FetcherRequest, nthreads),
	}
	p.start()
	return p
}

func (s *ParallelShuffleFetcher) start() {
	//TODO: just use go
	go func() {
		wg := make(chan bool, s.nthreads) //simple pools
		for req := range s.requests {
			wg <- true
			go func(r *_FetcherRequest) {
				defer func() { <-wg }()
				if d, err := s.fetch_one(r); err != nil {
					r.response <- err
				} else {
					r.response <- d
				}
			}(req)
		}
	}()
}

type _FetcherRequest struct {
	uri       string
	shuffleId uint64
	part      int
	reduceId  uint64
	response  chan interface{}
}

func (s *ParallelShuffleFetcher) fetch_one(req *_FetcherRequest) (map[int]interface{}, error) {
	uri, shuffleId := req.uri, req.shuffleId
	part, reduceId := req.part, req.reduceId
	url := ""
	var data []byte
	if uri == LocalFileShuffle.getServerUri() {
		// http get can not open local file
		url = LocalFileShuffle.getOutputFile(shuffleId, uint64(part), reduceId, 0)
		glog.Infof("fetch %s", url)
		if body, err := ioutil.ReadFile(url); err != nil {
			glog.Fatal(err)
		} else {
			data = body
		}
	} else {
		url = fmt.Sprintf("%s/%d/%d/%d", uri, shuffleId, part, reduceId)
		glog.Infof("fetch %s", url)
		resp, err := http.Get(url)
		if err != nil {
			glog.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == 404 {
			return nil, fmt.Errorf(" %s not found", url)
		}
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			glog.Fatal(err)
		} else {
			data = body
		}
	}

	if reader, err := gzip.NewReader(bytes.NewBuffer(data)); err != nil {
		glog.Fatal(err)
	} else {
		if bufs, err := ioutil.ReadAll(reader); err != nil {
			glog.Fatal(err)
		} else {
			var val map[int]interface{}
			dec := gob.NewDecoder(bytes.NewBuffer(bufs))
			if err := dec.Decode(&val); err != nil {
				glog.Fatal(err)
			}
			return val, nil
		}
	}
	return nil, nil
}

func (s *ParallelShuffleFetcher) Fetch(shuffleId uint64, reduceId uint64, merge func(<-chan interface{})) {
	glog.Infof("Fetching outputs for shuffle %d, reduce %d", shuffleId, reduceId)
	serverUris := _env.mapOutputTracker.GetServerUris(shuffleId)
	if serverUris == nil || len(serverUris) == 0 {
		return
	}
	type _Part struct {
		Idx int
		Uri string
	}
	urisLen := len(serverUris)
	parts := make([]_Part, urisLen)
	perm := rand.Perm(urisLen) //shuffle uris
	responses := make([]chan interface{}, urisLen)
	for i, v := range perm {
		u := serverUris[i]
		parts[v] = _Part{
			Idx: i,
			Uri: u,
		}
		responses[i] = make(chan interface{})
	}
	go func() {
		for _, v := range parts {
			part, uri := v.Idx, v.Uri
			req := &_FetcherRequest{
				uri:       uri,
				shuffleId: shuffleId,
				part:      part,
				reduceId:  reduceId,
				response:  responses[part],
			}
			s.requests <- req
		}
	}()

	wg := new(sync.WaitGroup)
	for i := 0; i < urisLen; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp := <-responses[idx]
			if err, ok := resp.(error); ok {
				glog.Fatal(err)
			} else if vals, ok := resp.(map[int]interface{}); ok {
				ch := make(chan interface{})
				go func() {
					defer close(ch)
					for _, v := range vals {
						ch <- v
					}
				}()
				merge(ch)
			} else {
				glog.Fatal("we do not know it.")
			}
		}(i)
	}
	wg.Wait()
}

func (s *ParallelShuffleFetcher) Stop() {
	glog.Info("stop parallel shuffle fetcher ...")
	close(s.requests)
}

type BaseMapOutputTracker interface {
	RegisterMapOutputs(shuffleId uint64, locs []string)
	GetServerUris(uint64) []string
	Stop()
}

type LocalMapOutputTracker struct {
	serverUris map[uint64][]string
}

func NewLocalMapOutputTracker() *LocalMapOutputTracker {
	return &LocalMapOutputTracker{
		serverUris: make(map[uint64][]string),
	}
}

func (t *LocalMapOutputTracker) clear() {
	t.serverUris = make(map[uint64][]string)
}

func (t *LocalMapOutputTracker) RegisterMapOutputs(shuffleId uint64, locs []string) {
	t.serverUris[shuffleId] = locs
}

func (t *LocalMapOutputTracker) GetServerUris(shuffleId uint64) []string {
	if vals, ok := t.serverUris[shuffleId]; ok {
		return vals
	}
	return nil
}

func (t *LocalMapOutputTracker) Stop() {
	t.clear()
}

type MapOutputTracker struct {
	client *_TrackerClient
}

func NewMapOutputTracker() *MapOutputTracker {
	return &MapOutputTracker{
		client: _env.trackerClient,
	}
}

func (t *MapOutputTracker) RegisterMapOutputs(shuffleId uint64, locs []string) {
	key := fmt.Sprintf("%d", shuffleId)
	if len(locs) > 0 {
		t.client.Set(key, locs)
	}
}

func (t *MapOutputTracker) GetServerUris(shuffleId uint64) []string {
	key := fmt.Sprintf("%d", shuffleId)
	if vals, err := t.client.Get(key); err != nil {
		glog.Fatal(err)
	} else {
		if vals != nil && len(vals) > 0 {
			return vals
		}
	}
	return make([]string, 0)
}

func (t *MapOutputTracker) Stop() {
}

type _Merger struct {
	mergeCombiners func(interface{}, interface{}) interface{}
	combined       map[interface{}]interface{}
	lock           sync.Mutex
}

func newMerger(total int, mergeCombiners func(interface{}, interface{}) interface{}) *_Merger {
	return &_Merger{
		mergeCombiners: mergeCombiners,
		combined:       make(map[interface{}]interface{}),
	}
}

func (m *_Merger) merge_it(k, v interface{}) {
	combined := m.combined
	mergeCombiners := m.mergeCombiners
	m.lock.Lock() //can remove ?
	defer m.lock.Unlock()
	if o, ok := combined[k]; ok {
		combined[k] = mergeCombiners(o, v)
	} else {
		combined[k] = v
	}
}

func (m *_Merger) merge(items <-chan interface{}) {
	for item := range items {
		kv := item.(*KeyValue)
		k := kv.Key
		v := kv.Value
		m.merge_it(k, v)
	}
}

func (m *_Merger) iter(done <-chan bool) <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		for k, v := range m.combined {
			select {
			case ch <- &KeyValue{
				Key:   k,
				Value: v,
			}:
			case <-done:
				return
			}
		}
	}()
	return ch
}
