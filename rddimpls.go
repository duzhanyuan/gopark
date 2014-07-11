package gopark

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"github.com/xiexiao/gopark/moosefs"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

func init() {
	gob.Register(&MappedRDD{})

	gob.Register(&UnionSplit{})
	gob.Register(&UnionRDD{})

	gob.Register(&ParallelSplit{})
	gob.Register(&ParallelRDD{})

	gob.Register(&FlatMappedRDD{})

	gob.Register(&PartialSplit{})
	gob.Register(&TextFileRDD{})

	gob.Register(&ShuffledSplit{})
	gob.Register(&ShuffledRDD{})

	gob.Register(&FilteredRDD{})

	gob.Register(&OutputTextFileRDD{})
}

//////////////////////////////////////////////////////
// DerivedRDD operations implementation
//////////////////////////////////////////////////////
type DerivedRDD struct {
	*BaseRDD
	Previous RDD
	Name     string
}

func newDerivedRDD(prevRdd RDD) *DerivedRDD {
	r := &DerivedRDD{
		BaseRDD:  newBaseRDD(prevRdd.getContext()),
		Previous: prevRdd,
	}
	r.Length = prevRdd.len()
	r.Splits = prevRdd.getSplits()
	t := reflect.TypeOf(r).Elem()
	r.Name = fmt.Sprintf("<%s %s>", t.Name(), r.Previous)
	r.dependencies = []Dependency{newOneToOneDependency(prevRdd)}
	return r
}

func (r *DerivedRDD) getSplit(index int) Spliter {
	return r.Previous.getSplit(index)
}

func (r *DerivedRDD) _getPreferredLocations(split Spliter) []string {
	return r.Previous.getPreferredLocations(split)
}

func (r *DerivedRDD) String() string {
	return r.Name
}
func (r *DerivedRDD) init(rdd RDD) {
	r.prototype = rdd
	r.Previous.init(r.Previous)
}

//////////////////////////////////////////////////////
// MappedRDD Impl
//////////////////////////////////////////////////////
type MappedRDD struct {
	*DerivedRDD
	Fn   *MapperFunc
	Name string
}

func (r *MappedRDD) String() string {
	return r.Name
}

func (r *MappedRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		fn := r.Fn.GetFunc()
		for value := range r.Previous.iterator(done, split) {
			ch <- fn(value)
		}
	}()
	return ch
}

func newMappedRDD(rdd RDD, fn *MapperFunc) RDD {
	r := &MappedRDD{
		DerivedRDD: newDerivedRDD(rdd),
		Fn:         fn,
	}
	r.DerivedRDD.init(r)
	t := reflect.TypeOf(r).Elem()
	r.Name = fmt.Sprintf("<%s %s>", t.Name(), r.Previous)
	return r
}

////////////////////////////////////////////////////////////////////////
// UnionRDD impl
////////////////////////////////////////////////////////////////////////
type UnionSplit struct {
	Index uint64
	Rdd   RDD
	Split Spliter
}

func (s *UnionSplit) getIndex() uint64 {
	return s.Index
}

type UnionRDD struct {
	*BaseRDD
	Size int
	Rdds []RDD
}

func (r *UnionRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	ch := make(chan interface{}, 1)
	go func() {
		unionSplit := split.(*UnionSplit)
		rdd := unionSplit.Rdd
		rdd.init(rdd)
		for value := range rdd.iterator(done, unionSplit.Split) {
			ch <- value
		}
		close(ch)
	}()
	return ch
}

func (r *UnionRDD) _getPreferredLocations(split Spliter) []string {
	unionSplit := split.(*UnionSplit)
	rdd := unionSplit.Rdd
	rdd.init(rdd)
	return rdd.getPreferredLocations(split)
}

func newUnionRDD(ctx *Context, rdds []RDD) RDD {
	r := &UnionRDD{
		BaseRDD: newBaseRDD(ctx),
	}
	r.BaseRDD.init(r)
	r.Size = len(rdds)
	r.Rdds = rdds
	for _, rdd := range rdds {
		r.Length += rdd.len()
	}
	r.Splits = make([]Spliter, r.Length)
	var index uint64 = 0
	pos := 0
	for _, rdd := range rdds {
		length := rdd.len()
		for _, split := range rdd.getSplits() {
			r.Splits[index] = &UnionSplit{
				Index: index,
				Rdd:   rdd,
				Split: split.(Spliter),
			}
			index++
			r.dependencies = append(r.dependencies, newRangeDependency(rdd, 0, pos, length))
		}
		pos += length
	}
	return r
}

func (r *UnionRDD) String() string {
	return fmt.Sprintf("<UnionRDD %d %s ...>", r.Size, r.Rdds[0])
}

////////////////////////////////////////////////////////////////////////
// ParallelRDD Impl
////////////////////////////////////////////////////////////////////////
type ParallelSplit struct {
	Index  uint64
	Values []interface{}
}

func (s *ParallelSplit) getIndex() uint64 {
	return s.Index
}

type ParallelRDD struct {
	*BaseRDD
}

func (r *ParallelRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	_split := split.(*ParallelSplit)
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		for d := range _split.Values {

			select {
			case ch <- d:
			case <-done:
				return
			}
		}
	}()
	return ch
}

func (r *ParallelRDD) _getPreferredLocations(split Spliter) []string {
	return nil
}

func newParallelRDD(ctx *Context, data []interface{}, numSlices int) RDD {
	r := &ParallelRDD{
		BaseRDD: newBaseRDD(ctx),
	}
	r.BaseRDD.init(r)
	var taskMemory float64 = -1
	if taskMemory > 0 {
		r.Mem = taskMemory
	}
	r.Length = len(data)
	if r.Length <= 0 {
		glog.Fatal("Please don't provide an empty data array.")
	}
	sliceSize := MinInt(r.Length, numSlices)
	if sliceSize < 1 {
		sliceSize = 1
	}
	slices := r.slice(data, sliceSize)
	count := len(slices)
	r.Splits = make([]Spliter, count)
	for i := 0; i < count; i++ {
		r.Splits[i] = &ParallelSplit{
			Index:  uint64(i),
			Values: slices[i],
		}
	}
	r.dependencies = []Dependency{}
	return r
}

func (r *ParallelRDD) slice(data []interface{}, numSlices int) [][]interface{} {
	if numSlices <= 0 {
		glog.Fatalf("invalid numSlices %d", numSlices)
	}
	m := len(data)
	if m == 0 {
		return nil
	}
	n := m / numSlices
	if m%numSlices != 0 {
		n += 1
	}
	splits := make([][]interface{}, numSlices)
	for i := 0; i < numSlices; i++ {
		end := n*i + n
		if end > m-1 {
			end = r.Length
		}
		if n*i < r.Length {
			splits[i] = data[n*i : end]
		}
	}
	return splits
}

func (r *ParallelRDD) String() string {
	return fmt.Sprintf("<ParallelRDD %d>", r.Length)
}

////////////////////////////////////////////////////////////////////////
// TextFileRDD Impl
////////////////////////////////////////////////////////////////////////
const DEFAULT_FILE_SPLIT_SIZE = 64 * 1024 * 1024 // 64MB Split Size

type PartialSplit struct {
	Index uint64
	Begin int64
	End   int64
}

func (s *PartialSplit) getIndex() uint64 {
	return s.Index
}

type TextFileRDD struct {
	*BaseRDD
	Path      string
	Size      int64
	SplitSize int64
	fileInfo  *moosefs.File
}

func (r *TextFileRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	ch := make(chan interface{}, 100)
	go func() {
		defer close(ch)
		f, err := r.open_file()
		if err != nil {
			panic(err)
		}
		defer f.Close()

		_split := split.(*PartialSplit)
		start := _split.Begin
		end := _split.End
		if start > 0 {
			start, err = seekToNewLine(f, start-1)
			if err == io.EOF {
				return
			}
			if err != nil {
				panic(err)
			}
		}

		if start >= end {
			return
		}
		reader := bufio.NewReader(f)
		line, err := readLine(reader)
		for err == nil {
			select {
			case ch <- line:
			case <-done:
				return
			}
			start += int64(len(line)) + 1 // here would be dargon
			if start >= end {
				break
			}
			line, err = readLine(reader)
		}
		if err != nil && err != io.EOF {
			panic(err)
		}
	}()
	return ch
}

func (r *TextFileRDD) open_file() (gpFile, error) {
	if r.fileInfo != nil {
		//DONE:it's mfs files
		return moosefs.NewReadableFile(r.fileInfo), nil
	} else {
		return os.Open(r.Path)
	}
}

func (r *TextFileRDD) String() string {
	return fmt.Sprintf("<TextFileRDD %s>", r.Path)
}

func (r *TextFileRDD) _getPreferredLocations(split Spliter) []string {
	return nil
	/*
	   if not self.fileinfo:
	           return []

	       if self.splitSize != moosefs.CHUNKSIZE:
	           start = split.begin / moosefs.CHUNKSIZE
	           end = (split.end + moosefs.CHUNKSIZE - 1)/ moosefs.CHUNKSIZE
	           return sum((self.fileinfo.locs(i) for i in range(start, end)), [])
	       else:
	           return self.fileinfo.locs(split.begin / self.splitSize)*/
}

func newTextFileRDD(ctx *Context, path string, numSplits int) RDD {
	r := &TextFileRDD{
		BaseRDD: newBaseRDD(ctx),
	}
	r.BaseRDD.init(r)
	r.Path = path

	fi2, _ := moosefs.OpenFile(path)
	r.fileInfo = fi2

	fi, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	r.Size = fi.Size()
	r.SplitSize = DEFAULT_FILE_SPLIT_SIZE
	if numSplits > 0 {
		r.SplitSize = r.Size / int64(numSplits)
	}
	if r.Size > 0 {
		r.Length = int(r.Size / r.SplitSize)
	} else {
		r.Length = 0
	}
	r.Splits = make([]Spliter, r.Length)
	for i := 0; i < r.Length; i++ {
		end := int64(i+1) * r.SplitSize
		if i == r.Length-1 {
			end = r.Size
		}
		r.Splits[i] = &PartialSplit{
			Index: uint64(i),
			Begin: int64(i) * r.SplitSize,
			End:   end,
		}
	}
	return r
}

////////////////////////////////////////////////////////////////////////
// FlatMappedRDD Impl
////////////////////////////////////////////////////////////////////////
type FlatMappedRDD struct {
	*DerivedRDD
	Fn *FlatMapperFunc
}

func (r *FlatMappedRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	yield := make(chan interface{}, 1)
	go func() {
		fn := r.Fn.GetFunc()
		for arg := range r.Previous.iterator(done, split) {
			value := fn(arg)
			if value != nil && len(value) > 0 {
				for _, subValue := range value {
					yield <- subValue
				}
			}
		}
		close(yield)
	}()
	return yield
}

func (r *FlatMappedRDD) String() string {
	return fmt.Sprintf("<FlatMappedRDD %s>", r.DerivedRDD.Previous)
}

func newFlatMappedRDD(rdd RDD, fn *FlatMapperFunc) RDD {
	r := &FlatMappedRDD{
		DerivedRDD: newDerivedRDD(rdd),
		Fn:         fn,
	}
	r.DerivedRDD.init(r)
	return r
}

////////////////////////////////////////////////////////////////////////
// ShuffledRDD Impl
////////////////////////////////////////////////////////////////////////

type ShuffledSplit struct {
	Index uint64
}

func (s *ShuffledSplit) getIndex() uint64 {
	return s.Index
}

type ShuffledRDD struct {
	*BaseRDD
	ShuffleId   uint64
	Parent      RDD
	Aggregator  *Aggregator
	Partitioner Partitioner
	NumParts    int
}

func (r *ShuffledRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	mergeCombiners := r.Aggregator.MergeCombiners.GetFunc()
	merger := newMerger(r.NumParts, mergeCombiners)
	fetcher := _env.shuffleFetcher
	fetcher.Fetch(r.ShuffleId, split.getIndex(), merger.merge)
	return merger.iter(done)
}

func (r *ShuffledRDD) String() string {
	return fmt.Sprintf("<ShuffledRDD %s>", r.Parent)
}

func (r *ShuffledRDD) _getPreferredLocations(split Spliter) []string {
	return nil
}

var _shuffleId uint64 = 0

func newShuffledRDD(rdd RDD, aggregator *Aggregator, part Partitioner) RDD {
	r := &ShuffledRDD{
		BaseRDD: newBaseRDD(rdd.getContext()),
	}
	r.BaseRDD.init(r)

	r.ShuffleId = atomic.AddUint64(&_shuffleId, 1)
	r.Parent = rdd
	r.NumParts = rdd.len()
	r.Aggregator = aggregator
	r.Partitioner = part
	r.Length = part.numPartitions()

	r.Splits = make([]Spliter, r.Length)
	for i := 0; i < r.Length; i++ {
		r.Splits[i] = &ShuffledSplit{Index: uint64(i)}
	}
	r.dependencies = []Dependency{
		newShuffleDependency(r.ShuffleId, rdd, aggregator, part)}
	return r
}

////////////////////////////////////////////////////////////////////////
// FilteredRDD Impl
////////////////////////////////////////////////////////////////////////
type FilteredRDD struct {
	*DerivedRDD
	Fn *FilterFunc
}

func (r *FilteredRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	ch := make(chan interface{}, 1)
	go func() {
		defer close(ch)
		fn := r.Fn.GetFunc()
		for value := range r.Previous.iterator(done, split) {
			if fn(value) {
				ch <- value
			}
		}
	}()
	return ch
}

func (r *FilteredRDD) String() string {
	return fmt.Sprintf("<FilteredRDD %s>", r.DerivedRDD.Previous)
}

func newFilteredRDD(rdd RDD, fn *FilterFunc) RDD {
	r := &FilteredRDD{
		DerivedRDD: newDerivedRDD(rdd),
		Fn:         fn,
	}
	r.DerivedRDD.init(r)
	return r
}

////////////////////////////////////////////////////////////////////////
// OutputTextFileRDD Impl
////////////////////////////////////////////////////////////////////////
type OutputTextFileRDD struct {
	*DerivedRDD
	Path      string
	Ext       string
	Overwrite bool
	Compress  bool
}

func (r *OutputTextFileRDD) compute(done <-chan bool, split Spliter) <-chan interface{} {
	dpath := path.Join(r.Path,
		fmt.Sprintf("%04d%s", split.getIndex(), r.Ext))
	if _, ok := ExistPath(dpath); ok && !r.Overwrite {
		return nil
	}
	hostname, _ := os.Hostname()
	tpath := path.Join(r.Path,
		fmt.Sprintf(".%04d%s.%s.%d.tmp", split.getIndex(), r.Ext,
			hostname, os.Getpid()))
	var f *os.File
	var err error
	if f, err = os.OpenFile(tpath, os.O_CREATE, os.ModePerm); err != nil {
		time.Sleep(1 * time.Second) // there are dir cache in mfs for 1 sec
		if f, err = os.OpenFile(tpath, os.O_CREATE, os.ModePerm); err != nil {
			glog.Fatal(err)
		}
	}
	have_data := false
	data := r.Previous.iterator(done, split)
	if r.Compress {
		have_data = r.write_compress_data(f, data)
	} else {
		have_data = r.write_data(f, data)
	}
	f.Close()
	ch := make(chan interface{})
	if _, ok := ExistPath(dpath); !ok && have_data {
		os.Rename(tpath, dpath)
		defer os.RemoveAll(tpath)
		go func() {
			defer close(ch)
			ch <- dpath
		}()
	}
	return ch
}

func (r *OutputTextFileRDD) write_data(f *os.File, data <-chan interface{}) bool {
	lastLine := ""
	defer f.Close()
	for line := range data {
		if err, ok := line.(error); ok {
			glog.Fatal(err)
			return false
		} else {
			if lastLine == "" {
				lastLine = line.(string)
			} else {
				f.WriteString(lastLine)
				if !strings.HasPrefix(lastLine, "\n") {
					f.WriteString("\n")
				}
				lastLine = ""
			}
		}
	}
	if lastLine != "" {
		f.WriteString(lastLine)
	}
	return true
}

func (r *OutputTextFileRDD) write_compress_data(f *os.File, data <-chan interface{}) bool {
	gz := gzip.NewWriter(f)
	defer gz.Close()
	lastLine := ""
	size := 0
	for line := range data {
		if err, ok := line.(error); ok {
			glog.Fatal(err)
			return false
		} else {
			if lastLine == "" {
				lastLine = line.(string)
			} else {
				d := []byte(lastLine)
				size += len(d) + 1
				gz.Write(d)
				if !strings.HasPrefix(lastLine, "\n") {
					f.Write([]byte{'\n'})
				}
				lastLine = ""
				if size >= 256<<10 {
					gz.Flush()
					size = 0
				}
			}
		}
	}
	if lastLine != "" {
		d := []byte(lastLine)
		f.Write(d)
	}
	return true
}

func (r *OutputTextFileRDD) String() string {
	return fmt.Sprintf("<OutputTextFileRDD %s %s>", r.Path, r.DerivedRDD.Previous)
}

func newOutputTextFileRDD(rdd RDD, dpath, ext string, overwrite, compress bool) RDD {
	r := &OutputTextFileRDD{
		DerivedRDD: newDerivedRDD(rdd),
		Overwrite:  overwrite,
		Compress:   compress,
	}

	createDir := false
	if fi, ok := ExistPath(dpath); ok {
		if !fi.IsDir() {
			glog.Fatal("output must be dir")
		}
		if overwrite {
			createDir = true
			if err := os.RemoveAll(dpath); err != nil {
				glog.Fatal(err)
			}
		}
	} else {
		createDir = true
	}
	if createDir {
		if err := os.MkdirAll(dpath, os.ModePerm); err != nil {
			glog.Fatal(err)
		}
	}
	if absPath, err := filepath.Abs(dpath); err != nil {
		glog.Fatal(err)
	} else {
		r.Path = absPath
	}
	if ext != "" && !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	if compress && !strings.HasSuffix(ext, "gz") {
		ext = ext + ".gz"
	}
	r.Ext = ext
	r.DerivedRDD.init(r)
	return r
}
