package gopark

import (
	"fmt"
	"sync/atomic"
)

var _ = fmt.Println

type AccumulateFunc func(x, y interface{}) interface{}
type GetZeroFunc func() interface{}

type AccumulatorParam interface {
	AddFunc() AccumulateFunc
	GetZero() interface{}
}
type accumulatorParam struct {
	fn      AccumulateFunc
	zeroFun GetZeroFunc
}

func (ap accumulatorParam) AddFunc() AccumulateFunc {
	return ap.fn
}

func (ap accumulatorParam) GetZero() interface{} {
	return ap.zeroFun()
}

var float64AccumulatorParam = &accumulatorParam{
	fn: func(x, y interface{}) interface{} {
		return x.(float64) + y.(float64)
	},
	zeroFun: func() interface{} {
		return float64(0)
	},
}

var listAccumulatorParam = &accumulatorParam{
	fn: func(x, y interface{}) interface{} {
		return append(x.([]interface{}), y)
	},
	zeroFun: func() interface{} {
		return make([]interface{}, 0)
	},
}

var mapAccumulatorParam = &accumulatorParam{
	fn: func(x, y interface{}) interface{} {
		m := y.(map[interface{}]interface{})
		w := x.(map[interface{}]interface{})
		for k, v := range m {
			w[k] = v
		}
		return w
	},
	zeroFun: func() interface{} {
		return make(map[interface{}]interface{})
	},
}

//Accumulator

type Accumulator interface {
	GetId() uint64
	Add(interface{})
	Value() interface{}
	Reset() interface{}
}

type _BaseAccumulator struct {
	id       uint64
	param    AccumulatorParam
	value    interface{}
	accuChan chan interface{}
}

func (a *_BaseAccumulator) init(initValue interface{}, param AccumulatorParam) {
	a.id = accumulators.NewId()
	a.value = initValue
	a.param = param
	a.accuChan = make(chan interface{})
	go func() {
		for {
			localValue := <-a.accuChan
			a.value = a.param.AddFunc()(a.value, localValue)
		}
	}()
	accumulators.Register(a, true)
}

func (a *_BaseAccumulator) GetId() uint64 {
	return a.id
}

func (a *_BaseAccumulator) Add(x interface{}) {
	a.accuChan <- x
	accumulators.Register(a, false)
}

func (a *_BaseAccumulator) Value() interface{} {
	return a.value
}

func (a *_BaseAccumulator) Reset() interface{} {
	v := a.value
	zero := a.param.GetZero()
	a.value = zero
	return v
}

func newAccumulator(initValue interface{}, param AccumulatorParam) Accumulator {
	a := &_BaseAccumulator{}
	a.init(initValue, param)
	return a
}

func NewFloat64Accumulator(initValue float64) Accumulator {
	return newAccumulator(initValue, float64AccumulatorParam)
}

func NewListAccumulator(initValue []interface{}) Accumulator {
	if initValue == nil {
		vals := make([]interface{}, 0)
		return newAccumulator(vals, listAccumulatorParam)
	} else {
		return newAccumulator(initValue, listAccumulatorParam)
	}
}

func NewMapAccumulator(initValue map[interface{}]interface{}) Accumulator {
	return newAccumulator(initValue, mapAccumulatorParam)
}

var accumulators = _NewAccumulators()

type _Accumulators struct {
	originals   map[uint64]Accumulator
	localAccums map[uint64]Accumulator
	newId       chan uint64
}

func _NewAccumulators() *_Accumulators {
	acc := &_Accumulators{
		originals:   make(map[uint64]Accumulator),
		localAccums: make(map[uint64]Accumulator),
		newId:       make(chan uint64),
	}
	var id uint64 = 0
	go func() {
		for {
			acc.newId <- atomic.AddUint64(&id, 1)
		}
	}()
	return acc
}

func (accs *_Accumulators) NewId() uint64 {
	return <-accs.newId
}

func (accs *_Accumulators) Register(a Accumulator, originals bool) {
	id := a.GetId()
	if originals {
		accs.originals[id] = a
	} else {
		accs.localAccums[id] = a
	}
}

func (accs *_Accumulators) Clear() {
	for _, val := range accs.localAccums {
		val.Reset()
	}
	accs.localAccums = make(map[uint64]Accumulator)
}

func (accs *_Accumulators) Values() map[uint64]interface{} {
	vals := make(map[uint64]interface{})
	for idx, val := range accs.localAccums {
		vals[idx] = val.Value()
	}
	accs.Clear()
	return vals
}

func (accs *_Accumulators) Merge(values map[uint64]interface{}) {
	for idx, val := range values {
		accs.originals[idx].Add(val)
	}
}
