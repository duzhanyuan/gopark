package gopark

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/xiexiao/gopark/mesos"
	"net"
	"sync/atomic"
	"time"
)

var (
	TASK_STARTING = 0
	TASK_RUNNING  = 1
	TASK_FINISHED = 2
	TASK_FAILED   = 3
	TASK_KILLED   = 4
	TASK_LOST     = 5
)

type Jobber interface {
	getId() uint64
	slaveOffer(host string, availableCpus, availableMem float64) _Tasker
	statusUpdate(tid, tried uint64,
		state mesos.TaskState, reason error,
		result interface{}, update map[uint64]interface{})
}

var (
	LOCALITY_WAIT     float64 = 0
	WAIT_FOR_RUNNING          = 10
	MAX_TASK_FAILURES         = 4
	MAX_TASK_MEMORY           = 15 << 10 // 15GB
)

type SimpleJob struct {
	id    uint64
	sched Scheduler
	start time.Time
	tasks map[uint64]_Tasker

	launched    map[uint64]bool
	finished    map[uint64]bool
	numFailures map[uint64][]int
	blacklist   map[uint64][]string

	numTasks      int
	tasksLaunched int
	tasksFinished int
	total_used    float64

	lastPreferredLaunchTime time.Time

	pendingTasksForHost     map[string][]uint64
	pendingTasksWithNoPrefs map[uint64]bool
	allPendingTasks         map[uint64]bool

	reasons        []string
	failed         bool
	causeOfFailure string
	last_check     int
	host_cache     map[string][]uint64
}

var _jobId uint64 = 0

func newSimpleJob(sched Scheduler, tasks []_Tasker, cpus float64, mem float64) Jobber {
	id := atomic.AddUint64(&_jobId, 1)
	numTasks := len(tasks)
	j := &SimpleJob{
		id:    id,
		sched: sched,
		start: time.Now(),
		tasks: make(map[uint64]_Tasker),

		launched:    make(map[uint64]bool),
		finished:    make(map[uint64]bool),
		numFailures: make(map[uint64][]int),
		blacklist:   make(map[uint64][]string),

		numTasks:      numTasks,
		tasksLaunched: 0,
		tasksFinished: 0,
		total_used:    0,

		lastPreferredLaunchTime: time.Now(),

		pendingTasksForHost:     make(map[string][]uint64),
		pendingTasksWithNoPrefs: make(map[uint64]bool),
		allPendingTasks:         make(map[uint64]bool),

		reasons:        make([]string, 0),
		failed:         false,
		causeOfFailure: "",
		last_check:     0,
		host_cache:     make(map[string][]uint64),
	}
	for _, t := range tasks {
		tid := t.getId()
		t.setStatus("")
		t.setTried(0)
		t.setUsed(0)
		t.setCpus(cpus)
		t.setMem(mem)
		j.tasks[tid] = t

		j.launched[tid] = false
		j.finished[tid] = false
		j.numFailures[tid] = []int{0}
		j.blacklist[tid] = make([]string, 0)

		j.addPendingTask(tid)
	}
	return j
}

func (j *SimpleJob) addPendingTask(tid uint64) {
	loc := j.tasks[tid].preferredLocations()
	if loc == nil || len(loc) == 0 {
		j.pendingTasksWithNoPrefs[tid] = true
	} else {
		for _, host := range loc {
			if v, ok := j.pendingTasksForHost[host]; ok {
				j.pendingTasksForHost[host] = append(v, tid)
			} else {
				j.pendingTasksForHost[host] = []uint64{tid}
			}
		}
		j.allPendingTasks[tid] = true
	}
}

func (j *SimpleJob) getId() uint64 {
	return j.id
}

func (j *SimpleJob) findTaskFromList(list []uint64, host string, cpus, mem float64) uint64 {
	for _, tid := range list {
		if j.launched[tid] {
			continue
		}
		if j.finished[tid] {
			continue
		}
		for _, _host := range j.blacklist[tid] {
			if _host == host {
				continue
			}
		}
		t := j.tasks[tid]
		if t.getCpus() <= cpus+1e-4 && t.getMem() <= mem {
			return tid
		}
	}
	return 0
}

func (j *SimpleJob) getPendingTasksForHost(host string) []uint64 {
	if v, ok := j.host_cache[host]; ok {
		return v
	} else {
		v = j._getPendingTasksForHost(host)
		j.host_cache[host] = v
		return v
	}
}

func (j *SimpleJob) _getPendingTasksForHost(host string) []uint64 {
	hosts := []string{host}
	tasks := make([]uint64, 0)
	vs, _ := net.LookupIP(host)
	for _, v := range vs {
		if v.To4() != nil {
			hosts = append(hosts, fmt.Sprintf("%s", v))
		}
	}

	for _, h := range hosts {
		if values, ok := j.pendingTasksForHost[h]; ok {
			for _, val := range values {
				tasks = append(tasks, val)
			}
		}
	}
	st_map := make(map[uint64]*KeyValue)
	for _, t := range tasks {
		if _kv, ok := st_map[t]; ok {
			_kv.Value = _kv.Value.(int) + 1
		} else {
			st_map[t] = &KeyValue{t, 1}
		}
	}
	st := make([]interface{}, len(st_map))
	for idx, kv := range st_map {
		st[idx] = kv
	}
	ts := mergeSort(st, func(x, y interface{}) bool {
		if x.(*KeyValue).Value.(int) < y.(*KeyValue).Value.(int) {
			return true
		}
		return false
	}, true) // max --> min
	results := make([]uint64, len(ts))
	for idx, v := range ts {
		results[idx] = v.(*KeyValue).Key.(uint64)
	}
	return results
}

func (j *SimpleJob) findTask(host string, localOnly bool, cpus, mem float64) (uint64, bool) {
	localTask := j.findTaskFromList(j.getPendingTasksForHost(host), host, cpus, mem)
	if localTask != 0 {
		return localTask, true
	}
	noPrefsIds := make([]uint64, len(j.pendingTasksWithNoPrefs))
	i := 0
	for p, _ := range j.pendingTasksWithNoPrefs {
		noPrefsIds[i] = p
		i += 1
	}
	noPrefTask := j.findTaskFromList(noPrefsIds, host, cpus, mem)
	if noPrefTask != 0 {
		return noPrefTask, true
	}
	pendingIds := make([]uint64, len(j.allPendingTasks))
	i = 0
	for p, _ := range j.allPendingTasks {
		pendingIds[i] = p
		i += 1
	}
	if !localOnly {
		return j.findTaskFromList(pendingIds, host, cpus, mem), false
	} else {
		//TODO:log it ?
	}
	return 0, false
}

// Respond to an offer of a single slave from the scheduler by finding a task
func (j *SimpleJob) slaveOffer(host string, availableCpus, availableMem float64) _Tasker {
	now := time.Now()
	localOnly := now.Sub(j.lastPreferredLaunchTime).Seconds() < LOCALITY_WAIT
	tid, preferred := j.findTask(host, localOnly, availableCpus, availableMem)
	glog.Infof("slaveOffer: localOnly: %#v, tid: %d", localOnly, tid)
	if tid != 0 {
		task := j.tasks[tid]
		task.setStatus("TASK_STARTING")
		task.setStart(now)
		task.setHost(host)
		task.setTried(task.getTried() + 1)
		prefStr := "non-preferred"
		if preferred {
			prefStr = "preferred"
		}
		glog.Infof("Starting task %d:%d as TID %d on slave %s (%s)",
			j.getId(), 0, tid, host, prefStr)
		j.launched[tid] = true
		j.tasksLaunched += 1
		j.blacklist[tid] = append(j.blacklist[tid], host)
		if preferred {
			j.lastPreferredLaunchTime = now
		}
		return task
	}
	glog.Infof("no task found %t", localOnly)
	return nil
}

func (j *SimpleJob) statusUpdate(tid, tried uint64,
	state mesos.TaskState, reason error,
	result interface{}, update map[uint64]interface{}) {
	state_msg := mesos.TaskState_name[int32(state)]
	glog.Infof("job status update %d %s %#v", tid, state_msg, reason)
	if v, ok := j.finished[tid]; ok && v {
		if state == mesos.TaskState_TASK_FINISHED {
			glog.Infof("Task %d is already finished, ignore it", tid)
		}
		return
	} else {
		if task, ok := j.tasks[tid]; ok {
			task.setStatus(state_msg)
			// when checking, task been masked as not launched
			if _, ok := j.launched[tid]; !ok {
				j.launched[tid] = true
				j.tasksLaunched += 1
			}
			if state == mesos.TaskState_TASK_FINISHED {
				glog.Infof("job status update %d %s %#v %#v", tid, state_msg, reason, result)
				j.taskFinished(tid, tried, result, update)
			} else if state == mesos.TaskState_TASK_LOST ||
				state == mesos.TaskState_TASK_FAILED ||
				state == mesos.TaskState_TASK_KILLED {
				j.taskLost(tid, tried, state, reason)
			}
			task.setStart(time.Now())
		} else {
			glog.Errorf("invalid tid: %d", tid)
			return
		}
	}
}

func (j *SimpleJob) taskFinished(tid, tried uint64, result interface{}, update map[uint64]interface{}) {
	j.finished[tid] = true
	j.tasksFinished += 1
	task := j.tasks[tid]

	used := task.getUsed() + time.Now().Sub(task.getStart()).Seconds()
	task.setUsed(used)
	j.total_used += task.getUsed()
	j.sched.taskEnded(task, nil, result, update)
	glog.Infof("taskFinished: %d, j.numTasks: %d", j.tasksFinished, j.numTasks)
	if j.tasksFinished == j.numTasks {
		ts := make([]float64, j.numTasks)
		tried := make([]uint64, j.numTasks)
		i := 0
		for _, t := range j.tasks {
			ts[i] = t.getUsed()
			tried[i] = t.getTried()
			i = i + 1
		}
		d := time.Now().Sub(j.start)
		glog.Infof("Job %d finished in %.1fs: min=%.1fs, avg=%.1fs, max=%.1fs, maxtry=%d",
			j.id, d.Seconds(),
			MinFloat64s(ts), sumFloat64(ts)/float64(len(ts)), MaxFloat64s(ts), MaxUint64s(tried))
		j.sched.jobFinished(j)
	}
}

func (j *SimpleJob) taskLost(tid, tried uint64, state mesos.TaskState, reason error) {
	glog.Fatal("taskLost")
}
