package gopark

import (
	"fmt"
	"github.com/golang/glog"
	"path"
	"sync/atomic"
)

type RDD interface {
	Map(fn *MapperFunc) RDD
	Reduce(fn *ReducerFunc) interface{}
	Foreach(fn *LoopFunc)
	FlatMap(fn *FlatMapperFunc) RDD
	ReduceByKey(fn *ReducerFunc) RDD
	Filter(fn *FilterFunc) RDD

	Top10(fn *LessFunc) []interface{}
	TopN(n int64, fn *LessFunc) []interface{}
	Collect() []interface{}
	CollectAsMap() map[interface{}]interface{}
	Count() int64

	Cache() RDD

	SaveAsTextFile(dpath string) []interface{}
	SaveAsTextFileM(dpath string, ext string, overwrite bool, compress bool) []interface{}
	//TODO:SaveAsTableFile(dpath string) RDD
	//TODO:SaveAsTableFileM(dpath string, overwrite bool) RDD

	init(rdd RDD)

	getId() uint64
	getMem() float64
	getShouldCache() bool
	getDependencies() []Dependency
	getContext() *Context

	getSplits() []Spliter
	getSplit(int) Spliter
	getPreferredLocations(split Spliter) []string
	_getPreferredLocations(split Spliter) []string

	len() int
	iterator(done <-chan bool, split Spliter) <-chan interface{}
	compute(done <-chan bool, split Spliter) <-chan interface{}
}

type Spliter interface {
	getIndex() uint64
}
type BaseRDD struct {
	prototype    RDD
	ctx          *Context
	dependencies []Dependency
	cache        [][]interface{}

	Id            uint64
	Splits        []Spliter
	ShouldCache   bool
	Length        int
	Mem           float64
	Snapshot_path string
}

//////////////////////////////////////////////////////
// Base RDD operations implementation
//////////////////////////////////////////////////////

var _rddId uint64 = 0

func newBaseRDD(ctx *Context) *BaseRDD {
	r := &BaseRDD{}
	r.ctx = ctx
	r.Id = atomic.AddUint64(&_rddId, 1)
	r.Splits = make([]Spliter, 0)
	r.dependencies = make([]Dependency, 0)

	r.ctx.init()
	return r
}

func (r *BaseRDD) getId() uint64 {
	return r.Id
}

func (r *BaseRDD) getMem() float64 {
	return r.Mem
}

func (r *BaseRDD) getContext() *Context {
	return r.ctx
}

func (r *BaseRDD) getSplits() []Spliter {
	return r.Splits
}

func (r *BaseRDD) getSplit(index int) Spliter {
	return r.Splits[index].(Spliter)
}
func (r *BaseRDD) getPreferredLocations(split Spliter) []string {
	if r.ShouldCache {
		locs := _env.cacheTracker.getCachedLocs(r.Id, uint64(split.getIndex()))
		if locs != nil {
			return locs
		}
	}
	return r.prototype._getPreferredLocations(split)
}

func (r *BaseRDD) len() int {
	return len(r.Splits)
}

func (r *BaseRDD) getShouldCache() bool {
	return r.ShouldCache
}
func (r *BaseRDD) getDependencies() []Dependency {
	return r.dependencies
}

func (r *BaseRDD) iterator(done <-chan bool, split Spliter) <-chan interface{} {
	if r.Snapshot_path != "" {
		//TODO:do snapshot
		p := path.Join(r.Snapshot_path, fmt.Sprintf("%d", split.getIndex()))
		if _, ok := ExistPath(p); ok {
		} else {
		}
	}
	rdd := r.prototype //TODO:if nil
	glog.Info("ShouldCache :", r.ShouldCache, rdd)
	if r.ShouldCache {
		return _env.cacheTracker.getOrCompute(done, rdd, split)
	} else {
		return rdd.compute(done, split)
	}
}

func (r *BaseRDD) init(prototype RDD) {
	r.prototype = prototype
}

var gp_foreachFunc = newJobFunc("gp_foreachFunc", func(yield <-chan interface{},
	partition int, additions []interface{}) (interface{}, error) {
	do := additions[0].(*LoopFunc).GetFunc()
	for value := range yield {
		do(value)
	}
	return nil, nil
})

func init() {
	RegisterFunc(gp_foreachFunc)
}

func (r *BaseRDD) Cache() RDD {
	if !r.ShouldCache {
		r.ShouldCache = true
		//TODO:r.cache = make([][]interface{}, r.Length)
	}
	return r.prototype
}

func (r *BaseRDD) Foreach(fn *LoopFunc) {
	done := make(chan bool)
	defer close(done)
	dumps := r.ctx.runJob(done, r.prototype, gp_foreachFunc, []interface{}{fn}, nil, false)
	for _ = range dumps {
	}
}

func (r *BaseRDD) Map(fn *MapperFunc) RDD {
	return newMappedRDD(r.prototype, fn)
}

var gp_reduceFunc = newJobFunc("gp_reduceFunc",
	func(yield <-chan interface{}, partition int, additions []interface{}) (interface{}, error) {
		do := additions[0].(*ReducerFunc).GetFunc()
		var accu interface{} = nil
		for value := range yield {
			switch {
			case accu == nil:
				accu = value
			default:
				accu = do(accu, value)
			}
		}
		return accu, nil
	})

func init() {
	RegisterFunc(gp_reduceFunc)
}

func (r *BaseRDD) Reduce(fn *ReducerFunc) interface{} {
	done := make(chan bool)
	defer close(done)
	iters := r.ctx.runJob(done, r.prototype, gp_reduceFunc, []interface{}{fn}, nil, false)
	do := fn.GetFunc()
	var accu interface{} = nil
	for value := range iters {
		if value != nil {
			switch {
			case accu == nil:
				accu = value
			default:
				accu = do(accu, value)
			}
		}
	}
	return accu
}

var gp_collect = newJobFunc("gp_collect",
	func(ch <-chan interface{}, partition int, _ []interface{}) (interface{}, error) {
		subCollections := make([]interface{}, 0)
		for value := range ch {
			subCollections = append(subCollections, value)
		}
		return subCollections, nil
	})

func init() {
	RegisterFunc(gp_collect)
}
func (r *BaseRDD) Collect() []interface{} {
	done := make(chan bool)
	defer close(done)
	iters := r.ctx.runJob(done, r.prototype, gp_collect, nil, nil, false)
	collections := make([]interface{}, 0)
	for iter := range iters {
		subCollections := iter.([]interface{})
		collections = append(collections, subCollections...)
	}
	return collections
}

func (r *BaseRDD) CollectAsMap() map[interface{}]interface{} {
	collections := r.Collect()
	sets := make(map[interface{}]interface{})
	for _, item := range collections {
		kv := item.(*KeyValue)
		sets[kv.Key] = kv.Value
	}
	return sets
}

func (r *BaseRDD) FlatMap(fn *FlatMapperFunc) RDD {
	return newFlatMappedRDD(r.prototype, fn)
}

func (r *BaseRDD) Filter(fn *FilterFunc) RDD {
	return newFilteredRDD(r.prototype, fn)
}

func (r *BaseRDD) ReduceByKey(fn *ReducerFunc) RDD {
	return r.ReduceByKey_N(fn, 0)
}

var gp_reduceByKey = NewMapperFunc("gp_reduceByKey", func(x interface{}) interface{} {
	return x
})

func init() {
	RegisterFunc(gp_reduceByKey)
}

func (r *BaseRDD) ReduceByKey_N(fn *ReducerFunc, numPartitions int) RDD {
	aggregator := &Aggregator{gp_reduceByKey, fn, fn}
	return r.combineByKey(aggregator, numPartitions)
}

func (r *BaseRDD) combineByKey(aggregator *Aggregator, numPartitions int) RDD {
	if numPartitions <= 0 {
		switch {
		case _env.parallel == 0:
			numPartitions = r.len()
		default:
			numPartitions = _env.parallel
		}
	}
	patitioner := newHashPartitioner(numPartitions)
	return newShuffledRDD(r.prototype, aggregator, patitioner)
}

var gopark_count = newJobFunc("gopark_count",
	func(yield <-chan interface{}, partition int, additions []interface{}) (interface{}, error) {
		var total int64 = 0
		for _ = range yield {
			total++
		}
		return total, nil
	})

func init() {
	RegisterFunc(gopark_count)
}

func (r *BaseRDD) Count() int64 {
	var cnt int64 = 0
	done := make(chan bool)
	defer close(done)
	iters := r.ctx.runJob(done, r.prototype, gopark_count, nil, nil, false)
	for subCount := range iters {
		cnt += subCount.(int64)
	}
	return cnt
}

func (r *BaseRDD) Top10(fn *LessFunc) []interface{} {
	return r.TopN(10, fn)
}

var gp_topn = newJobFunc("gp_topn", func(yield <-chan interface{}, partition int, additions []interface{}) (interface{}, error) {
	do := additions[0].(*LessFunc).GetFunc()
	reverse := additions[1].(bool)
	N1 := additions[2].(int64)

	var idx int64 = 0
	results := make([]interface{}, N1) //TODO:when size is zero
	for val := range yield {
		if idx < N1 {
			results[idx] = val
		} else if idx == N1 {
			results = mergeSort(results, do, reverse)
		} else {
			results = mergeSort(results, do, reverse)
			results[N1-1] = val
		}
		idx = idx + 1
	}

	if idx == 0 {
		return nil, nil
	} else if idx < N1 {
		return results[:idx], nil
	} else {
		return results, nil
	}
})

func init() {
	RegisterFunc(gp_topn)
}

func (r *BaseRDD) TopN(n int64, fn *LessFunc) []interface{} {
	N := n
	reverse := false
	if n < 0 {
		N = -n
		reverse = true
	} else if n == 0 {
		return nil
	}
	N1 := N + 1
	do := fn.GetFunc()
	done := make(chan bool)
	defer close(done)
	iters := r.ctx.runJob(done, r.prototype, gp_topn, []interface{}{fn, reverse, N1}, nil, false)
	var idx int64 = 0
	results := make([]interface{}, N1)
	needLastSort := true
	for vals := range iters {
		if vals != nil {
			for _, val := range vals.([]interface{}) {
				if idx < N1 {
					results[idx] = val
				} else if idx == N1 {
					results = mergeSort(results, do, reverse)
					needLastSort = false
				} else {
					results = mergeSort(results, do, reverse)
					results[N] = val
				}
				idx = idx + 1
			}
		}
	}
	if idx == 0 {
		return nil
	} else if idx < N {
		results = mergeSort(results, do, reverse)
		return results[:idx]
	} else {
		if needLastSort {
			results = mergeSort(results, do, reverse)
		}
		return results[:N]
	}
}

func (r *BaseRDD) SaveAsTextFile(dpath string) []interface{} {
	return r.SaveAsTextFileM(dpath, "", true, false)
}

func (r *BaseRDD) SaveAsTextFileM(dpath, ext string, overwrite, compress bool) []interface{} {
	return newOutputTextFileRDD(r.prototype, dpath, ext, overwrite, compress).Collect()
}
