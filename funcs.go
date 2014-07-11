package gopark

import (
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
)

func init() {
	gob.Register(&KeyValue{})
	gob.Register(&MapperFunc{})     //func(interface{}) interface{}
	gob.Register(&LoopFunc{})       //func(interface{})
	gob.Register(&LessFunc{})       //func(interface{}, interface{}) bool
	gob.Register(&ReducerFunc{})    //func(interface{}, interface{}) interface{}
	gob.Register(&FlatMapperFunc{}) //func(interface{}) []interface{}
	gob.Register(&FilterFunc{})     //func(interface{}) bool
}

var _funcMap = make(map[string]interface{})

//RegisterFunc must use in func init()
func RegisterFunc(f Funcer) {
	name := f.GetName()
	fn := f.GetBaseFunc()
	if _, ok := _funcMap[name]; ok {
		panic(fmt.Sprintf("RegisterFunc[%s] already exists!", name))
	}
	_funcMap[name] = fn
}

//GetFunc get the func register
func _getFunc(name string) interface{} {
	if fn, ok := _funcMap[name]; ok {
		return fn
	} else {
		return nil
	}
}

//Func define
type Funcer interface {
	GetName() string
	GetBaseFunc() interface{}
}

type BaseFunc struct {
	Name  string
	_func interface{}
}

func newBaseFunc(name string, fn interface{}) *BaseFunc {
	return &BaseFunc{
		Name:  name,
		_func: fn,
	}
}

func (f *BaseFunc) GetName() string {
	return f.Name
}

func (f *BaseFunc) GetBaseFunc() interface{} {
	if f._func == nil {
		if f.Name != "" {
			if fn := _getFunc(f.Name); fn == nil {
				glog.Fatalf("Func[%s] not Register!", f.Name)
			} else {
				return fn
			}
		} else {
			glog.Fatal("Func not Register!")
		}
	}
	return f._func
}

//====JobFuncer

type JobFunc struct {
	*BaseFunc
}

func newJobFunc(name string, fn func(yield <-chan interface{},
	partition int, additions []interface{}) (interface{}, error)) *JobFunc {
	return &JobFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}

func (f *JobFunc) GetFunc() func(yield <-chan interface{},
	partition int, additions []interface{}) (interface{}, error) {
	return f.BaseFunc.GetBaseFunc().(func(yield <-chan interface{},
		partition int, additions []interface{}) (interface{}, error))
}

//====LessFunc

type LessFunc struct {
	*BaseFunc
}

func NewLessFunc(name string, fn func(interface{}, interface{}) bool) *LessFunc {
	return &LessFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}

func (f *LessFunc) GetFunc() func(interface{}, interface{}) bool {
	return f.BaseFunc.GetBaseFunc().(func(interface{}, interface{}) bool)
}

//====LoopFunc

type LoopFunc struct {
	*BaseFunc
}

func NewLoopFunc(name string, fn func(interface{})) *LoopFunc {
	return &LoopFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}
func (f *LoopFunc) GetFunc() func(interface{}) {
	return f.BaseFunc.GetBaseFunc().(func(interface{}))
}

//====MapperFunc

type MapperFunc struct {
	*BaseFunc
}

func NewMapperFunc(name string, fn func(interface{}) interface{}) *MapperFunc {
	return &MapperFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}
func (f *MapperFunc) GetFunc() func(interface{}) interface{} {
	return f.BaseFunc.GetBaseFunc().(func(interface{}) interface{})
}

//====ReducerFunc

type ReducerFunc struct {
	*BaseFunc
}

func NewReducerFunc(name string, fn func(interface{}, interface{}) interface{}) *ReducerFunc {
	return &ReducerFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}
func (f *ReducerFunc) GetFunc() func(interface{}, interface{}) interface{} {
	return f.BaseFunc.GetBaseFunc().(func(interface{}, interface{}) interface{})
}

//====FlatMapperFunc
type FlatMapperFunc struct {
	*BaseFunc
}

func NewFlatMapperFunc(name string, fn func(interface{}) []interface{}) *FlatMapperFunc {
	return &FlatMapperFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}
func (f *FlatMapperFunc) GetFunc() func(interface{}) []interface{} {
	return f.BaseFunc.GetBaseFunc().(func(interface{}) []interface{})
}

//====FilterFunc
type FilterFunc struct {
	*BaseFunc
}

func NewFilterFunc(name string, fn func(interface{}) bool) *FilterFunc {
	return &FilterFunc{
		BaseFunc: newBaseFunc(name, fn),
	}
}

func (f *FilterFunc) GetFunc() func(interface{}) bool {
	return f.BaseFunc.GetBaseFunc().(func(interface{}) bool)
}

//====KeyValue

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

func (kv *KeyValue) String() string {
	return fmt.Sprintf("%v:%v", kv.Key, kv.Value)
}

type KeyGroups struct {
	Key    interface{}
	Groups [][]interface{}
}

type KeyLessFunc func(x, y interface{}) bool

type ParkSorter struct {
	values []interface{}
	fn     KeyLessFunc
}

func (s *ParkSorter) Len() int {
	return len(s.values)
}

func (s *ParkSorter) Swap(i, j int) {
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

func (s *ParkSorter) Less(i, j int) bool {
	return s.fn(s.values[i], s.values[j])
}

func NewParkSorter(values []interface{}, fn KeyLessFunc) *ParkSorter {
	return &ParkSorter{values, fn}
}

type JobFn func(yield <-chan interface{}, partition int, fn Funcer) (interface{}, error)

type gpFile interface {
	Close() error
	Read(b []byte) (n int, err error)
	Name() string
	Seek(offset int64, whence int) (ret int64, err error)
}
