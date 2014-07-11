package gopark

import (
	"encoding/gob"
	"sort"
)

func init() {
	gob.Register(&HashPartitioner{})
}

type Aggregator struct {
	CreateCombiner *MapperFunc
	MergeValue     *ReducerFunc
	MergeCombiners *ReducerFunc
}

type Partitioner interface {
	numPartitions() int
	getPartition(key interface{}) int
}

type HashPartitioner struct {
	Partitions int
}

func (p *HashPartitioner) numPartitions() int {
	return p.Partitions
}

func (p *HashPartitioner) getPartition(key interface{}) int {
	hashCode := HashCode(key)
	return int(hashCode % int64(p.Partitions))
}

func newHashPartitioner(partitions int) Partitioner {
	p := &HashPartitioner{}
	if partitions < 1 {
		p.Partitions = 1
	} else {
		p.Partitions = partitions
	}
	return p
}

type RangePartitioner struct {
	Keys    []interface{}
	Reverse bool
	Fn      KeyLessFunc
}

func (p *RangePartitioner) numPartitions() int {
	return len(p.Keys) + 1
}

func (p *RangePartitioner) getPartition(key interface{}) int {
	index := sort.Search(len(p.Keys), func(i int) bool {
		return !p.Fn(p.Keys[i], key)
	})
	if !p.Reverse {
		return index
	}
	return len(p.Keys) - index
}

func newRangePartitioner(fn KeyLessFunc, keys []interface{}, reverse bool) Partitioner {
	p := &RangePartitioner{}
	p.Fn = fn
	p.Reverse = reverse
	sorter := NewParkSorter(keys, fn)
	sort.Sort(sorter)
	p.Keys = keys
	return p
}
