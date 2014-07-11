package gopark

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"sync/atomic"
	"time"
)

func init() {
	gob.Register(&ResultTask{})
	gob.Register(&ShuffleMapTask{})
}

type _Tasker interface {
	getId() uint64
	getRdd() RDD
	getStageId() uint64
	run(done <-chan bool, id uint64) (interface{}, error)
	preferredLocations() []string

	getCpus() float64
	setCpus(float64)
	getMem() float64
	setMem(float64)
	getStatus() string
	setStatus(string)

	getTried() uint64
	setTried(uint64)
	getUsed() float64
	setUsed(float64)

	getHost() string
	setHost(string)
	getStart() time.Time
	setStart(time.Time)
}

type DAGTask struct {
	Id   uint64
	Locs []string

	Cpus   float64
	Mem    float64
	Used   float64
	Tried  uint64
	Status string
	Start  time.Time
	Host   string

	StageId uint64
	Rdd     RDD
}

var _taskId uint64 = 0

func newDAGTask(stageId uint64, rdd RDD, locs []string) *DAGTask {
	id := atomic.AddUint64(&_taskId, 1)
	return &DAGTask{
		Id:      id,
		Rdd:     rdd,
		StageId: stageId,
		Locs:    locs,
	}
}

func (t *DAGTask) getId() uint64 {
	return t.Id
}

func (t *DAGTask) getCpus() float64 {
	return t.Cpus
}
func (t *DAGTask) setCpus(val float64) {
	t.Cpus = val
}

func (t *DAGTask) getMem() float64 {
	return t.Mem
}
func (t *DAGTask) setMem(val float64) {
	t.Mem = val
}

func (t *DAGTask) getStatus() string {
	return t.Status
}
func (t *DAGTask) setStatus(val string) {
	t.Status = val
}

func (t *DAGTask) getTried() uint64 {
	return t.Tried
}
func (t *DAGTask) setTried(val uint64) {
	t.Tried = val
}

func (t *DAGTask) getUsed() float64 {
	return t.Used
}
func (t *DAGTask) setUsed(val float64) {
	t.Used = val
}

func (t *DAGTask) getStart() time.Time {
	return t.Start
}
func (t *DAGTask) setStart(val time.Time) {
	t.Start = val
}

func (t *DAGTask) getHost() string {
	return t.Host
}
func (t *DAGTask) setHost(val string) {
	t.Host = val
}

func (t *DAGTask) getRdd() RDD {
	return t.Rdd
}

func (t *DAGTask) getStageId() uint64 {
	return t.StageId
}

func (t *DAGTask) preferredLocations() []string {
	return t.Locs
}

type BTask struct {
	Data interface{}
}

type ResultTask struct {
	*DAGTask
	Split     Spliter
	Run       *JobFunc
	Additions []interface{}
	Partition int
	OutputId  int
}

func newResultTask(stageId uint64, rdd RDD, run *JobFunc,
	additions []interface{},
	partition int, locs []string, outputId int) *ResultTask {
	return &ResultTask{
		DAGTask:   newDAGTask(stageId, rdd, locs),
		Run:       run,
		Additions: additions,
		Partition: partition,
		Split:     rdd.getSplit(partition),
		OutputId:  outputId,
	}
}
func (t *ResultTask) run(done <-chan bool, attemptId uint64) (interface{}, error) {
	glog.Infof("run task %s with %d", t, attemptId)
	rdd := t.Rdd
	rdd.init(rdd)
	run := t.Run.GetFunc()
	return run(rdd.iterator(done, t.Split), t.Partition, t.Additions)
}

func (t *ResultTask) String() string {
	return fmt.Sprintf("<ResultTask(%d) of %s>", t.Partition, t.Rdd)
}

type ShuffleMapTask struct {
	*DAGTask
	ShuffleId   uint64
	Aggregator  *Aggregator
	Partitioner Partitioner
	Partition   int
	Split       Spliter
	Locs        []string
}

func newShuffleMapTask(stageId uint64, rdd RDD, dep *_ShuffleDependency,
	partition int, locs []string) *ShuffleMapTask {
	return &ShuffleMapTask{
		DAGTask:     newDAGTask(stageId, rdd, locs),
		ShuffleId:   dep.shuffleId,
		Aggregator:  dep.aggregator,
		Partitioner: dep.partitioner,
		Partition:   partition,
		Split:       rdd.getSplit(partition),
	}
}

func (t *ShuffleMapTask) String() string {
	return fmt.Sprintf("<ShuffleMapTask(%d, %d) of %s>", t.ShuffleId, t.Partition, t.Rdd)
}

func (t *ShuffleMapTask) run(done <-chan bool, attemptId uint64) (interface{}, error) {
	glog.Infof("shuffling %d of %s", t.Partition, t.Rdd)
	numOutputSplits := t.Partitioner.numPartitions()
	getPartition := t.Partitioner.getPartition
	mergeValue := t.Aggregator.MergeValue.GetFunc()
	createCombiner := t.Aggregator.CreateCombiner.GetFunc()
	hostname, _ := os.Hostname()
	pid := os.Getpid()

	buckets := make([]map[int]interface{}, numOutputSplits)
	for i := 0; i < numOutputSplits; i++ {
		buckets[i] = make(map[int]interface{})
	}
	k := 0
	rdd := t.Rdd
	rdd.init(rdd)
	for v := range rdd.iterator(done, t.Split) {
		bucketId := getPartition(k)
		bucket := buckets[bucketId]
		if r, ok := bucket[k]; ok {
			bucket[k] = mergeValue(r, v)
		} else {
			bucket[k] = createCombiner(v)
		}
		k += 1
	}
	for i := 0; i < numOutputSplits; i++ {
		buffer := new(bytes.Buffer)
		if err := gob.NewEncoder(buffer).Encode(buckets[i]); err != nil {
			glog.Fatal(err)
		} else {
			dataBuf := new(bytes.Buffer)
			w := gzip.NewWriter(dataBuf)
			if _, err := w.Write(buffer.Bytes()); err != nil {
				glog.Fatal(err)
			} else {
				w.Close()
				data := dataBuf.Bytes()

				for tried := 0; tried < 4; tried++ {
					fpath := LocalFileShuffle.getOutputFile(t.ShuffleId, uint64(t.Partition),
						uint64(i), uint64(len(data)*tried))
					tpath := fpath + fmt.Sprintf(".%s.%d", hostname, pid)
					if ioutil.WriteFile(tpath, data, os.ModePerm); err == nil {
						if err := os.Rename(tpath, fpath); err != nil {
							glog.Fatal(err)
						}
						break
					} else {
						glog.Fatal(err)
					}
				}
			}
		}
	}
	uri := LocalFileShuffle.getServerUri()
	glog.Infof("run done on [%s].", uri)
	return uri, nil
}
