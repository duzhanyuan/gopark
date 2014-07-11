package gopark

import (
	"fmt"
	"github.com/golang/glog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type _SuccessError struct{}

func (e _SuccessError) Error() string {
	return ""
}

type _FetchFailedError struct{}

func (e _FetchFailedError) Error() string {
	return ""
}

var RESUBMIT_TIMEOUT = 60

type Stage struct {
	id            uint64
	rdd           RDD
	shuffleDep    *_ShuffleDependency
	parents       []*Stage
	numPartitions int
	outputLocs    [][]string
}

var _stageId uint64 = 0

func newStage(rdd RDD, shuffleDep *_ShuffleDependency, parents []*Stage) *Stage {
	id := atomic.AddUint64(&_stageId, 1)
	rddLen := rdd.len()
	return &Stage{
		id:            id,
		rdd:           rdd,
		shuffleDep:    shuffleDep,
		numPartitions: rddLen,
		parents:       parents,
		outputLocs:    make([][]string, rddLen),
	}
}

func (s *Stage) String() string {
	return fmt.Sprintf("<Stage(%d) for %s>", s.id, s.rdd)
}

func (s *Stage) isAvailable() bool {
	if (s.parents == nil || len(s.parents) == 0) &&
		s.shuffleDep == nil {
		return true
	}
	if s.outputLocs != nil && len(s.outputLocs) > 0 {
		for _, locs := range s.outputLocs {
			if locs == nil || len(locs) == 0 {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (s *Stage) allOuputLocs() bool {
	for _, v := range s.outputLocs {
		if v == nil || len(v) == 0 {
			return false
		}
	}
	return true
}

func (s *Stage) addOutputLoc(partition int, host string) {
	s.outputLocs[partition] = append(s.outputLocs[partition], host)
}

func (s *Stage) removeHost(host string) {
	becameUnavailable := false
	for idx, ls := range s.outputLocs {
		news := make([]string, 0)
		for _, l := range ls {
			if host == l {
				becameUnavailable = true
			} else {
				news = append(news, l)
			}
		}
		s.outputLocs[idx] = news
	}
	if becameUnavailable {
		glog.Infof("%s is now unavailable on host %s", s, host)
	}
}

type Scheduler interface {
	start()
	runJob(done <-chan bool, finalRdd RDD, rn *JobFunc,
		additions []interface{},
		partitions []int, allowLocal bool) <-chan interface{}
	taskEnded(task _Tasker, reason error,
		result interface{}, accumUpdates map[uint64]interface{})
	clear()
	stop()
	defaultParallelism() int
	jobFinished(_job interface{}) //just for mesos

	submitTasks(done <-chan bool, tasks []_Tasker)
	newStage(rdd RDD, shuffleDep *_ShuffleDependency) *Stage
	getShuffleMapStage(shuffleDep *_ShuffleDependency) *Stage
	updateCacheLocs()
	getMissingParentStages(stage *Stage) []*Stage
	getPreferredLocs(rdd RDD, partition int) []string
	getCompletionEvent() (*CompletionEvent, bool)
	getStage(id uint64) (*Stage, bool)
	check()
}

type CompletionEvent struct {
	task         _Tasker
	reason       error
	result       interface{}
	accumUpdates map[uint64]interface{}
}

type _DAGScheduler struct {
	completionLock    *sync.RWMutex
	completionEvents  []*CompletionEvent
	idToStage         map[uint64]*Stage
	shuffleToMapStage map[uint64]*Stage
	cacheLocs         map[uint64]map[uint64]*StringSet
}

func newDAGScheduler() *_DAGScheduler {
	d := &_DAGScheduler{
		idToStage:         make(map[uint64]*Stage),
		completionLock:    new(sync.RWMutex),
		shuffleToMapStage: make(map[uint64]*Stage),
	}
	d.completionLock.Lock()
	d.completionEvents = make([]*CompletionEvent, 0)
	d.completionLock.Unlock()
	return d
}

func (d *_DAGScheduler) getParentStages(rdd RDD) []*Stage {
	parents := make(map[uint64]*Stage)
	visited := make(map[uint64]bool)
	var visit func(r RDD)
	visit = func(r RDD) {
		if r == nil {
			return
		}
		rddId := r.getId()
		if _, ok := visited[rddId]; ok {
			return
		}
		visited[rddId] = true
		if r.getShouldCache() {
			_env.cacheTracker.registerRDD(rddId, uint64(r.len()))
		}
		for _, dep := range r.getDependencies() {
			if shDep, ok := dep.(*_ShuffleDependency); ok {
				sta := d.getShuffleMapStage(shDep)
				if _, ok := parents[sta.id]; !ok {
					parents[sta.id] = sta
				}
			} else {
				visit(dep.getRDD())
			}
		}
	}
	visit(rdd)
	results := make([]*Stage, len(parents))
	idx := 0
	for _, v := range parents {
		results[idx] = v
		idx += 1
	}
	return results
}
func (d *_DAGScheduler) getStage(id uint64) (*Stage, bool) {
	val, ok := d.idToStage[id]
	return val, ok
}

func (d *_DAGScheduler) newStage(rdd RDD, shuffleDep *_ShuffleDependency) *Stage {
	parents := d.getParentStages(rdd)
	stage := newStage(rdd, shuffleDep, parents)
	d.idToStage[stage.id] = stage
	glog.Infof("new stage: %s", stage)
	return stage

}

func (d *_DAGScheduler) getCacheLocs(r RDD) map[uint64]*StringSet {
	if val, ok := d.cacheLocs[r.getId()]; ok {
		return val
	} else {
		val = make(map[uint64]*StringSet)
		var i uint64
		count := uint64(r.len())
		for i = 0; i < count; i++ {
			val[i] = nil
		}
		return val
	}
}

func (d *_DAGScheduler) getCacheLocsAll(r RDD) bool {
	vals := d.getCacheLocs(r)
	for _, v := range vals {
		if v == nil || v.GetLength() == 0 {
			return false
		}
	}
	return true
}

func (d *_DAGScheduler) getShuffleMapStage(dep *_ShuffleDependency) *Stage {
	if stage, ok := d.shuffleToMapStage[dep.shuffleId]; !ok {
		stage = d.newStage(dep.rdd, dep)
		d.shuffleToMapStage[dep.shuffleId] = stage
		return stage
	} else {
		return stage
	}
}

func (d *_DAGScheduler) updateCacheLocs() {
	d.cacheLocs = _env.cacheTracker.getLocationsSnapshot()
}
func (d *_DAGScheduler) getPreferredLocs(rdd RDD, partition int) []string {
	split := rdd.getSplit(partition)
	return rdd.getPreferredLocations(split)
}

func (d *_DAGScheduler) taskEnded(task _Tasker, reason error,
	result interface{}, accumUpdates map[uint64]interface{}) {
	d.completionLock.Lock()
	d.completionEvents = append(d.completionEvents,
		&CompletionEvent{
			task, reason, result, accumUpdates,
		})
	d.completionLock.Unlock()
}

func (d *_DAGScheduler) getCompletionEvent() (*CompletionEvent, bool) {
	var result *CompletionEvent
	d.completionLock.Lock()
	defer d.completionLock.Unlock()
	length := len(d.completionEvents)
	if length > 0 {
		result = d.completionEvents[0]
		d.completionEvents = d.completionEvents[1:]
		return result, true
	}
	return nil, false
}

func (d *_DAGScheduler) getMissingParentStages(stage *Stage) []*Stage {
	missing := make(map[uint64]*Stage)
	visited := make(map[uint64]bool)
	var visit func(r RDD)
	visit = func(r RDD) {
		rddId := r.getId()
		if _, ok := visited[rddId]; ok {
			return
		}
		visited[rddId] = true

		if r.getShouldCache() && d.getCacheLocsAll(r) {
			return
		}
		for _, dep := range r.getDependencies() {
			switch inst := dep.(type) {
			case *_ShuffleDependency:
				sta := d.getShuffleMapStage(inst)
				if !sta.isAvailable() {
					if _, ok := missing[sta.id]; !ok {
						missing[sta.id] = sta
					}
				}
			case *_OneToOneDependency:
				visit(inst.getRDD())
			case *_NarrowDependency:
				glog.Info(inst)
				visit(inst.getRDD())
			}
		}
	}
	visit(stage.rdd)
	results := make([]*Stage, len(missing))
	idx := 0
	for _, v := range missing {
		results[idx] = v
		idx += 1
	}

	return results
}

func _runJob(d Scheduler, done <-chan bool, finalRdd RDD,
	jobFn *JobFunc, additions []interface{},
	partitions []int, allowLocal bool) <-chan interface{} {
	numOutputParts := len(partitions)
	outputParts := make([]int, numOutputParts)
	copy(outputParts, partitions)
	finalStage := d.newStage(finalRdd, nil)

	ch := make(chan interface{})
	go func() {
		finished := make(map[uint64]bool)
		numFinished := 0

		waiting := make(map[uint64]*Stage)
		running := make(map[uint64]*Stage)
		pendingTasks := make(map[uint64]map[uint64]bool)

		defer close(ch)
		d.updateCacheLocs()
		glog.Infof("Final stage: %s, %d", finalStage, numOutputParts)
		glog.Infof("Parents of final stage: %s", finalStage.parents)
		glog.Infof("Missing parents: %s", d.getMissingParentStages(finalStage))
		rn := jobFn.GetFunc()

		if allowLocal &&
			(finalStage.parents == nil ||
				len(finalStage.parents) == 0 ||
				d.getMissingParentStages(finalStage) == nil) &&
			numOutputParts == 1 {
			partion := outputParts[0]
			split := finalRdd.getSplit(partion)
			result, _ := rn(finalRdd.iterator(done, split), partion, additions)
			ch <- result
			return
		}
		var submitStage func(*Stage)
		var submitMissingTasks func(*Stage)
		submitStage = func(stage *Stage) {
			glog.Infof("submit stage %s", stage)
			stageId := stage.id
			_, ok1 := waiting[stageId]
			_, ok2 := running[stageId]
			if !ok1 && !ok2 {
				missing := d.getMissingParentStages(stage)
				if missing == nil || len(missing) == 0 {
					submitMissingTasks(stage)
					running[stageId] = stage
				} else {
					for _, parent := range missing {
						submitStage(parent)
					}
					waiting[stageId] = stage
				}
			}
		}

		submitMissingTasks = func(stage *Stage) {
			myPending := make(map[uint64]bool)
			stageId := stage.id
			pendingTasks[stageId] = myPending
			tasks := make([]_Tasker, 0)
			have_prefer := true
			if stage == finalStage {
				for i := 0; i < numOutputParts; i++ {
					if _, ok := finished[uint64(i)]; !ok {
						part := outputParts[i]
						var locs []string
						if have_prefer {
							locs = d.getPreferredLocs(finalRdd, part)
							if locs == nil || len(locs) == 0 {
								have_prefer = false
							}
						}
						tasks = append(tasks, newResultTask(finalStage.id, finalRdd, jobFn, additions, part, locs, i))
					}
				}
			} else {
				for p := 0; p < stage.numPartitions; p++ {
					if loc := stage.outputLocs[p]; loc == nil {
						var locs []string
						if have_prefer {
							locs = d.getPreferredLocs(stage.rdd, p)
							if locs == nil || len(locs) == 0 {
								have_prefer = false
							}
						}
						tasks = append(tasks, newShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep, p, locs))
					}
				}
			}
			glog.Infof("add to pending %d tasks", len(tasks))
			for _, t := range tasks {
				tId := t.getId()
				if _, ok := myPending[tId]; !ok {
					myPending[tId] = true
				}
			}
			d.submitTasks(done, tasks)
		}

		submitStage(finalStage)
		for numFinished != numOutputParts {
			if evt, ok := d.getCompletionEvent(); !ok {
				d.check()
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				task, reason, result := evt.task, evt.reason, evt.result
				stageId := task.getStageId()
				taskId := task.getId()
				if stage, ok := d.getStage(stageId); !ok {
					glog.Fatal("error")
				} else {
					if reason == nil {
						if _, ok := pendingTasks[stageId]; !ok {
							continue // stage from other job
						}
						glog.Infof("remove from pending %s from %s", task, stage)
						delete(pendingTasks[stageId], taskId)
						accumulators.Merge(evt.accumUpdates)
						switch inst := task.(type) {
						case *ResultTask:
							_ = inst
							select {
							case ch <- result:
							case <-done:
								return
							}
							numFinished += 1
						case *ShuffleMapTask:
							stage.addOutputLoc(inst.Partition, evt.result.(string))
							if pens, ok := pendingTasks[stageId]; ok && len(pens) == 0 && stage.allOuputLocs() {
								glog.Infof("%s finished; looking for newly runnable stages", stage)
								delete(running, stageId)
								if stage.shuffleDep != nil {
									locLen := len(stage.outputLocs)
									locs := make([]string, locLen)
									for idx, l := range stage.outputLocs {
										locs[idx] = l[len(l)-1]
									}
									shuffleId := stage.shuffleDep.shuffleId
									_env.mapOutputTracker.RegisterMapOutputs(shuffleId, locs)
								}
								d.updateCacheLocs()
								newlyRunnable := make(map[uint64]*Stage)
								for k, v := range waiting {
									stas := d.getMissingParentStages(v)
									if stas == nil || len(stas) == 0 {
										newlyRunnable[k] = v
									}
								}
								for k, v := range newlyRunnable {
									delete(waiting, k)
									if _, ok := running[k]; !ok {
										running[k] = v
									}
								}
								glog.Infof("newly runnable: %#v, %#v", waiting, newlyRunnable)
								for _, sta := range newlyRunnable {
									submitMissingTasks(sta)
								}
							}
						default:
							glog.Fatalf("task %s failed: %s", task, reason)
						}
					} else {
						glog.Fatalf("task %s failed: %s", task, reason)
					}
				}
			}
		}
		return
	}()
	return ch
}

func (d *_DAGScheduler) clear() {
	d.idToStage = make(map[uint64]*Stage)
	d.shuffleToMapStage = make(map[uint64]*Stage)
	d.cacheLocs = make(map[uint64]map[uint64]*StringSet)
	_env.cacheTracker.clear()
	_env.mapOutputTracker.Stop()
}

func (d *_DAGScheduler) defaultParallelism() int {
	return 2
}

type _LocalScheduler struct {
	*_DAGScheduler
	attemptId    uint64
	numRoutines  int
	nextAttempId chan uint64
}

func newLocalScheduler(numRoutines int) *_LocalScheduler {
	if numRoutines < 0 {
		numRoutines = 2
	} else if numRoutines == 0 {
		numRoutines = runtime.NumCPU()
	}
	sch := &_LocalScheduler{
		_DAGScheduler: newDAGScheduler(),
		attemptId:     0,
		numRoutines:   numRoutines,
		nextAttempId:  make(chan uint64),
	}
	go func() {
		for {
			sch.nextAttempId <- atomic.AddUint64(&sch.attemptId, 1)
		}
	}()
	return sch
}

func (s *_LocalScheduler) start() {
}

func (s *_LocalScheduler) stop() {
}

func run_task(done <-chan bool, task _Tasker, aid uint64) (uint64, error, interface{}, map[uint64]interface{}) {
	glog.Infof("Running task %#v", task)
	result, err := task.run(done, aid)
	taskId := task.getId()
	return taskId, err, result, nil
}

func (d *_LocalScheduler) runJob(done <-chan bool, finalRdd RDD,
	rn *JobFunc, additions []interface{},
	partitions []int, allowLocal bool) <-chan interface{} {
	return _runJob(d, done, finalRdd, rn, additions, partitions, allowLocal)
}

func (d *_LocalScheduler) check() {
}

func (s *_LocalScheduler) submitTasks(done <-chan bool, tasks []_Tasker) {
	if len(tasks) == 0 {
		return
	} else {
		glog.Infof("submit tasks %s in LocalScheduler[Goroutines:%d]", tasks, s.numRoutines)
	}
	wg := make(chan bool, s.numRoutines) //simple pools
	for idx, task := range tasks {
		wg <- true
		go func(wg chan bool, t _Tasker, idx int) {
			defer func() { <-wg }()
			_, reason, result, update := run_task(done, t, <-s.nextAttempId)
			s.taskEnded(t, reason, result, update)
		}(wg, task, idx)
	}
}

func (s *_LocalScheduler) jobFinished(_job interface{}) {
}
