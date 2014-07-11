package gopark

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"os"
	"path"
	"strings"
)

type Cacher interface {
	Get(key string) ([]interface{}, bool)
	clear()
}

type Cache struct {
	data map[string][]interface{}
}

func NewCache() *Cache {
	return &Cache{
		data: make(map[string][]interface{}),
	}
}

func (c *Cache) Get(key string) ([]interface{}, bool) {
	if vals, ok := c.data[key]; ok {
		return vals, true
	}
	return nil, false
}

func (c *Cache) Put(key string, value interface{}) interface{} {
	if value == nil {
		delete(c.data, key)
		return nil
	}
	if vals, ok := c.data[key]; ok {
		c.data[key] = append(vals, value)
	} else {
		c.data[key] = []interface{}{value}
	}
	return value
}

func (c *Cache) Puts(key string, values []interface{}) []interface{} {
	if values == nil {
		delete(c.data, key)
		return nil
	}
	c.data[key] = values
	return values
}
func (c *Cache) clear() {
	c.data = make(map[string][]interface{})
}

// DiskCache
type _DiskCache struct {
	*Cache
	tracker _BaseCacheTracker
	root    string
}

func newDiskCache(tracker _BaseCacheTracker, path string) *_DiskCache {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, os.ModePerm)
	}
	return &_DiskCache{
		Cache:   NewCache(),
		tracker: tracker,
		root:    path,
	}
}

func (c *_DiskCache) getPath(key string) string {
	return path.Join(c.root, fmt.Sprintf("_%s", key))
}

func (c *_DiskCache) Get(key string) ([]interface{}, bool) {
	path := c.getPath(key)
	tp := fmt.Sprintf("%s.%d", path, os.Getpid())
	if _, err := os.Stat(tp); err == nil {
		if f, err := os.Open(tp); err == nil {
			defer f.Close()
			val, _ := c.load(f)
			return val, true
		}
	}
	return nil, false
}

func (c *_DiskCache) Put(key string, value interface{}) interface{} {
	if value == nil {
		c.Puts(key, nil)
		return nil
	}
	values := []interface{}{value}
	return c.Puts(key, values)
}

func (c *_DiskCache) Puts(key string, values []interface{}) []interface{} {
	p := c.getPath(key)
	if values == nil {
		os.Remove(p)
		return nil
	}
	c.save(p, values)
	return values
}

func (c *_DiskCache) Clear() {
	os.RemoveAll(c.root)
}

func (c *_DiskCache) load(f *os.File) ([]interface{}, error) {
	var items []interface{}
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}
func (c *_DiskCache) save(fpath string, items []interface{}) ([]interface{}, error) {
	tp := fmt.Sprintf("%s.%d", fpath, os.Getpid())
	if f, err := os.OpenFile(tp, os.O_CREATE, os.ModePerm); err == nil {
		defer f.Close()
		var buffer bytes.Buffer
		enc := gob.NewEncoder(&buffer)
		err := enc.Encode(items)
		if err != nil {
			return nil, err
		}
		data := buffer.Bytes()
		f.Write(data)
		os.Rename(tp, fpath)
		return items, nil
	} else {
		return nil, err
	}
}

type _BaseCacheTracker interface {
	getLocationsSnapshot() map[uint64]map[uint64]*StringSet
	getCachedLocs(rddId, index uint64) []string
	registerRDD(rddId uint64, numPartitions uint64)
	clear()
	getOrCompute(done <-chan bool, rdd RDD, split Spliter) <-chan interface{}
}

type StringSet struct {
	set map[string]bool
}

func NewStringSet() *StringSet {
	return &StringSet{
		set: make(map[string]bool),
	}
}

func (set *StringSet) GetLength() int {
	return len(set.set)
}

func (set *StringSet) GetList() []string {
	if set.set == nil {
		return nil
	}
	length := len(set.set)
	if length == 0 {
		return nil
	}
	results := make([]string, length)
	var idx = 0
	for x, _ := range set.set {
		results[idx] = x
		idx += 1
	}
	return results
}

func (set *StringSet) Add(item string) bool {
	_, found := set.set[item]
	set.set[item] = true
	return !found
}

func (set *StringSet) Remove(item string) bool {
	_, found := set.set[item]
	delete(set.set, item)
	return found
}
func (set *StringSet) Clear() {
	set.set = make(map[string]bool)
}

//LocalCacheTracker
type _LocalCacheTracker struct {
	cache *Cache
	locs  map[uint64]map[uint64]*StringSet
}

//func (t *LocalCacheTracker) Get(key string) []interface{} {
//   return t.Get(key)
//}

func newLocalCacheTracker() *_LocalCacheTracker {
	return &_LocalCacheTracker{
		cache: NewCache(),
		locs:  make(map[uint64]map[uint64]*StringSet),
	}
}

func (t *_LocalCacheTracker) registerRDD(rddId uint64, numPartitions uint64) {
	if _, ok := t.locs[rddId]; !ok {
		glog.Infof("Registering RDD ID %d with cache", rddId)
		urls := make(map[uint64]*StringSet)
		var i uint64
		for i = 0; i < numPartitions; i++ {
			urls[i] = NewStringSet()
		}
		t.locs[rddId] = urls
	}
}

func (t *_LocalCacheTracker) getLocationsSnapshot() map[uint64]map[uint64]*StringSet {
	return t.locs
}

func parseHostname(uri string) string {
	if strings.Index(uri, "http://") > -1 {
		h := strings.Split(strings.Split(uri, ":")[1], "/")
		if len(h) > 0 {
			return h[len(h)-1]
		}
	}
	return ""
}

func (t *_LocalCacheTracker) getCachedLocs(rddId, index uint64) []string {
	urls := t.getCacheUri(rddId, index)
	if urls == nil {
		return nil
	}
	results := make([]string, len(urls))
	for idx, x := range urls {
		results[idx] = parseHostname(x)
	}
	return results
}

func (t *_LocalCacheTracker) getCacheUri(rddId, index uint64) []string {
	set := t.locs[rddId][index]
	if set == nil {
		return nil
	}
	return set.GetList()
}

func (t *_LocalCacheTracker) addHost(rddId, index uint64, host string) {
	if urls, ok := t.locs[rddId]; ok {
		if _, ok := urls[index]; ok {
			urls[index].Add(host)
		}
	}
}

func (c *_LocalCacheTracker) clear() {
	c.cache.clear()
}

func (t *_LocalCacheTracker) removeHost(rddId, index uint64, host string) {
	if urls, ok := t.locs[rddId]; ok {
		if _, ok := urls[index]; ok {
			urls[index].Remove(host)
		}
	}
}

func (c *_LocalCacheTracker) getOrCompute(done <-chan bool, rdd RDD, split Spliter) <-chan interface{} {
	rddId := rdd.getId()
	splitIdx := split.getIndex()
	key := fmt.Sprintf("%d-%d", rddId, splitIdx)
	cachedVal, _ := c.cache.Get(key)
	if cachedVal != nil {
		glog.Infof("Found partition in cache! %s", key)
		ch := make(chan interface{})
		go func() {
			defer close(ch)
			for _, v := range cachedVal {
				select {
				case ch <- v:
				case <-done:
					return
				}
			}
		}()
		return ch
	} else {
		glog.Infof("partition not in cache, %s", key)
		vals := rdd.compute(done, split)
		ch := make(chan interface{})
		go func() {
			defer close(ch)
			for i := range vals {
				c.cache.Put(key, i)
				select {
				case ch <- i:
				case <-done:
					return
				}
			}
			serve_uri := _env.Get("SERVER_URI", nil)
			if serve_uri != nil {
				c.addHost(rdd.getId(), split.getIndex(), serve_uri.(string))
			}
		}()
		return ch
	}
}

type _CacheTracker struct {
	cache  *_DiskCache
	client *_TrackerClient
	rdds   map[uint64]uint64
	locs   map[string][]string
}

func newCacheTracker() *_CacheTracker {
	dirs := _env.Get("WORKDIR", nil)
	var cachedir string
	if dirs == nil {
		glog.Fatal("_env WORKDIR is nil")
	}
	if paths, ok := dirs.([]string); ok && len(paths) > 0 {
		cachedir = path.Join(paths[0], "cache")
	} else {
		glog.Fatal("_env WORKDIR not set")
	}

	tra := &_CacheTracker{
		client: _env.trackerClient,
	}

	tra.cache = newDiskCache(tra, cachedir)

	if _env.isMaster {
		tra.locs = _env.trackerServer.locs
	}

	tra.rdds = make(map[uint64]uint64)
	return tra
}
func (c *_CacheTracker) clear() {
	c.cache.clear()
}

func (c *_CacheTracker) registerRDD(rddId, numPartitions uint64) {
	c.rdds[rddId] = numPartitions
}

func (c *_CacheTracker) getLocationsSnapshot() map[uint64]map[uint64]*StringSet {
	/*
	   result := make(map[int]map[int]*)
	   for rddId, partitions := range c.rdds {
	       var items = make([][]int, partitions)
	       for idx := 0; idx < partitions; idx++ {
	           cachekey := fmt.Sprintf("cache:%d-%d", rddId, idx)
	           if vals, ok := c.locs[cachekey]; ok {
	               if vv, ok := vals.([]int); ok {
	                   items[idx] = vv
	               } else {
	                   items[idx] = nil
	               }
	           } else {
	               items[idx] = nil
	           }
	       }
	       result[rddId] = items
	   }
	   return result
	*/
	return nil
}

func (c *_CacheTracker) getCachedLocs(rddId, index uint64) []string {
	cachekey := fmt.Sprintf("cache:%d-%d", rddId, index)
	if urls, ok := c.locs[cachekey]; ok {
		if len(urls) > 0 {
			results := make([]string, len(urls))
			for idx, x := range urls {
				results[idx] = parseHostname(x)
			}
			return results
		}
	}
	return nil
}

func (c *_CacheTracker) getCacheUri(rddId, index uint64) []string {
	cachekey := fmt.Sprintf("cache:%d-%d", rddId, index)
	if val, err := c.client.Get(cachekey); err == nil {
		return val
	}
	return nil
}

func (c *_CacheTracker) addHost(rddId, index uint64, host string) bool {
	cachekey := fmt.Sprintf("cache:%d-%d", rddId, index)
	if val, err := c.client.Add(cachekey, host); err == nil {
		return val
	}
	return false
}

func (c *_CacheTracker) removeHost(rddId, index uint64, host string) bool {
	cachekey := fmt.Sprintf("cache:%d-%d", rddId, index)
	if val, err := c.client.RemoveItem(cachekey, host); err == nil {
		return val
	}
	return false
}

func (c *_CacheTracker) getOrCompute(done <-chan bool, rdd RDD, split Spliter) <-chan interface{} {
	rddId := rdd.getId()
	splitIdx := split.getIndex()
	key := fmt.Sprintf("%d-%d", rddId, splitIdx)
	cachedVal, _ := c.cache.Get(key)
	if cachedVal != nil {
		glog.Infof("Found partition in cache! %s", key)
		ch := make(chan interface{})
		go func() {
			defer close(ch)
			for _, v := range cachedVal {
				select {
				case ch <- v:
				case <-done:
					return
				}
			}
		}()
		return ch
	} else {
		glog.Infof("partition not in cache, %s", key)
		vals := rdd.compute(done, split)
		ch := make(chan interface{})
		go func() {
			defer close(ch)
			for i := range vals {
				c.cache.Put(key, i)
				select {
				case ch <- i:
				case <-done:
					return
				}
			}
			serve_uri := _env.Get("SERVER_URI", nil).(string)
			if serve_uri != "" {
				c.addHost(rdd.getId(), split.getIndex(), serve_uri)
			}
		}()
		return ch
	}
}
