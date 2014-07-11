package gopark

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"github.com/xiexiao/gopark/mesos"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	MAX_FAILED              = 3
	EXECUTOR_MEMORY float64 = 64 // cache
	MAX_IDLE_TIME   float64 = 60 * 30
)

type _MesosScheduler struct {
	*_DAGScheduler
	master           string
	cpus             float64
	mem              float64
	task_per_node    int
	group            []string
	options          *Options
	started          bool
	last_finish_time time.Time
	isRegistered     bool
	executor         *mesos.ExecutorInfo
	driver           *mesos.SchedulerDriver
	out_logger       string
	err_logger       string

	activeJobs      map[uint64]Jobber
	activeJobsQueue map[uint64]Jobber
	jobTasks        map[uint64]map[string]*mesos.TaskInfo
	slaveTasks      map[string]int
	slaveFailed     map[string]int
	taskIdToJobId   map[string]uint64
	taskIdToSlaveId map[string]string

	fileData []byte // current exe file's bytes
	fileUrl  string
	fileName string
}

func newMesosScheduler(master string, opt *Options) Scheduler {
	var group []string
	if opt.group != "" {
		group = []string{opt.group}
	}
	task_per_node := opt.parallel
	if task_per_node < 2 {
		task_per_node = 2
	}
	s := &_MesosScheduler{
		_DAGScheduler: newDAGScheduler(),
		master:        master,
		cpus:          opt.cpus,
		mem:           opt.mem,
		task_per_node: task_per_node,
		isRegistered:  false,
		group:         group,
		options:       opt,
	}
	if s.fileData == nil {
		s.fileName, s.fileUrl, s.fileData = getExeFileData()
	}
	s.init_job()
	if s.executor == nil {
		s.executor = s.getExecutorInfo()
	}
	return s
}

func (s *_MesosScheduler) init_job() {
	s.activeJobs = make(map[uint64]Jobber)
	s.activeJobsQueue = make(map[uint64]Jobber)
	s.jobTasks = make(map[uint64]map[string]*mesos.TaskInfo)
	s.slaveTasks = make(map[string]int)
	s.slaveFailed = make(map[string]int)
	s.taskIdToJobId = make(map[string]uint64)
	s.taskIdToSlaveId = make(map[string]string)
}

func (s *_MesosScheduler) Registered(driver *mesos.SchedulerDriver,
	frameworkId mesos.FrameworkID, masterInfo mesos.MasterInfo) {
	glog.Infof("connect to master %s:%d(%s), registered as %s",
		long2ip(*masterInfo.Ip), *masterInfo.Port, *masterInfo.Id,
		*frameworkId.Value)
	s.isRegistered = true
}

func (s *_MesosScheduler) getResource(res []*mesos.Resource, name string) *float64 {
	for _, r := range res {
		if *r.Name == name {
			return r.Scalar.Value
		}
	}
	return nil
}

func (s *_MesosScheduler) getAttribute(attrs []*mesos.Attribute, name string) *string {
	for _, r := range attrs {
		if *r.Name == name {
			return r.Text.Value
		}
	}
	return nil
}

func sumFloat64(items []float64) float64 {
	var total float64 = 0
	for _, val := range items {
		total += val
	}
	return total
}

func (s *_MesosScheduler) StatusUpdate(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
	glog.Info("Received task status1: " + *status.Message)
	tid := *status.TaskId.Value
	state := *status.State
	glog.Infof("status update: %s %s", tid, mesos.TaskState_name[int32(state)])
	jid := s.taskIdToJobId[tid]
	if job, ok := s.activeJobs[jid]; !ok {
		glog.Infof("Ignoring update from TID %s because its job is gone", tid)
		return
	} else {
		tids := strings.Split(tid, ":")
		task_id1, _ := strconv.Atoi(tids[1])
		tried1, _ := strconv.Atoi(tids[2])
		task_id := uint64(task_id1)
		tried := uint64(tried1)
		if state == mesos.TaskState_TASK_RUNNING {
			job.statusUpdate(task_id, tried, state, nil, nil, nil)
			return
		}
		delete(s.taskIdToJobId, tid)
		delete(s.jobTasks[jid], tid)

		slaveId := s.taskIdToSlaveId[tid]
		if v, ok := s.slaveTasks[slaveId]; ok {
			s.slaveTasks[slaveId] = v - 1
		}
		delete(s.taskIdToSlaveId, tid)

		jobResult := &SimpleJobResult{}
		dec := gob.NewDecoder(bytes.NewBuffer(status.Data))
		if err := dec.Decode(jobResult); err != nil {
			glog.Fatal(err)
		} else {
			reason := jobResult.Err
			result := jobResult.Result
			accUpdate := jobResult.Updates
			if (state == mesos.TaskState_TASK_FINISHED || state == mesos.TaskState_TASK_FAILED) && status.Data != nil {
				job.statusUpdate(task_id, tried, state,
					reason, result, accUpdate)
				return
			}
		}
		// killed, lost, load failed
		job.statusUpdate(task_id, tried, state, nil, nil, nil)
	}
}

func (s *_MesosScheduler) ResourceOffers(driver *mesos.SchedulerDriver,
	_offers []mesos.Offer) {

	offerSize := len(_offers)
	rf := mesos.Filters{}
	if len(s.activeJobs) == 0 {
		var seconds float64 = 60 * 5
		rf.RefuseSeconds = &seconds
		for _, o := range _offers {
			driver.LaunchTasks(o.Id, nil, rf) //TODO:when error
		}
		return
	}
	start := time.Now()
	offers := make([]mesos.Offer, offerSize) //shuffle offers
	for idx, i := range rand.Perm(offerSize) {
		offers[i] = _offers[idx]
	}
	cpus := make([]float64, offerSize)
	mems := make([]float64, offerSize)
	for idx, o := range offers {
		oid := *o.SlaveId.Value
		cpus[idx] = *s.getResource(o.Resources, "cpus")
		var e_mem float64 = EXECUTOR_MEMORY
		if v, ok := s.slaveTasks[oid]; ok && v > 0 {
			e_mem = 0
		}
		mems[idx] = *s.getResource(o.Resources, "mem") - e_mem
	}

	glog.Infof("get %d offers (%.2f cpus, %.2f mem), %d jobs",
		len(offers), sumFloat64(cpus), sumFloat64(mems), len(s.activeJobs))

	tasks := make(map[string][]*mesos.TaskInfo)
	for _, job := range s.activeJobsQueue {
		for {
			launchedTask := false
			for idx, o := range offers {
				slaveId := *o.SlaveId.Value
				sgroup := s.getAttribute(o.Attributes, "group")
				group := "none"
				if sgroup != nil {
					group = *sgroup
				}
				next := false
				if s.group != nil && len(s.group) > 0 {
					next = true
					for _, g := range s.group {
						if group == g {
							next = false
							break
						}
					}
				}
				if next {
					continue
				}
				if v, ok := s.slaveFailed[slaveId]; ok && v >= MAX_FAILED {
					continue
				}
				if v, ok := s.slaveTasks[slaveId]; ok && v >= s.task_per_node {
					continue
				}
				if mems[idx] < s.mem || cpus[idx]+1e-4 < s.cpus {
					continue
				}
				t := job.slaveOffer(*o.Hostname, cpus[idx], mems[idx])
				if t == nil {
					continue
				}
				task := s.createTask(&o, job, t, cpus[idx])
				oid := *o.Id.Value
				tid := *task.TaskId.Value
				if _, ok := tasks[oid]; ok {
					tasks[oid] = append(tasks[oid], task)
				} else {
					tasks[oid] = []*mesos.TaskInfo{task}
				}

				glog.Infof("dispatch %s into %s", t, *o.Hostname)
				jobId := job.getId()
				s.taskIdToJobId[tid] = jobId
				s.taskIdToSlaveId[tid] = slaveId
				if v, ok := s.slaveTasks[slaveId]; ok {
					s.slaveTasks[slaveId] = v + 1
				} else {
					s.slaveTasks[slaveId] = 1
				}
				cpus[idx] -= math.Min(cpus[idx], t.getCpus())
				mems[idx] = t.getMem()
				launchedTask = true
			}
			if !launchedTask {
				break
			}
		}
	}
	now := time.Now()
	used := now.Sub(start)
	if used.Seconds() > 10 {
		glog.Errorf("use too much time in slaveOffer: %0.2fs", used.Seconds())
	}
	for _, o := range offers {
		oid := *o.Id.Value
		tt := tasks[oid]
		if len(tt) == 0 {
			if err := driver.LaunchTasks(o.Id, nil); err != nil {
				glog.Fatal(err)
			}
		} else {
			if err := driver.LaunchTasks(o.Id, tt); err != nil {
				glog.Fatal(err)
			}
		}
	}
	tsTotal := 0
	for _, ts := range tasks {
		tsTotal += len(ts)
	}
	glog.Infof("reply with %d tasks, %.2f cpus %.2f mem left",
		tsTotal, sumFloat64(cpus), sumFloat64(mems))
}

func (s *_MesosScheduler) getExecutorInfo() *mesos.ExecutorInfo {
	uri := HttpServerForExecutor(s.fileUrl, s.fileData)
	command := fmt.Sprintf("./%s -mesos_executor=http -logtostderr=1", s.fileName)
	environ := _env.environ
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(environ); err != nil {
		glog.Fatal(err)
	}
	executor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
		Command: &mesos.CommandInfo{
			Value: proto.String(command),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{
					Value: &uri,
				},
			},
		},
		Data: buf.Bytes(),
	}
	return executor
}

func (s *_MesosScheduler) createTask(o *mesos.Offer,
	job Jobber, t _Tasker, available_cpus float64) *mesos.TaskInfo {
	jobId := job.getId()
	taskId := t.getId()
	tried := t.getTried()
	tid := fmt.Sprintf("%d:%d:%d", jobId, taskId, tried)
	var buff bytes.Buffer
	gw := gzip.NewWriter(&buff)
	var encoder = gob.NewEncoder(gw)
	jobTask := &SimpleJobTask{t}
	if err := encoder.Encode(jobTask); err != nil {
		glog.Fatal(err)
	}
	gw.Close()

	task := &mesos.TaskInfo{
		Name: proto.String(fmt.Sprintf("task %s", tid)),
		TaskId: &mesos.TaskID{
			Value: proto.String(tid),
		},
		Data:     buff.Bytes(),
		SlaveId:  o.SlaveId,
		Executor: s.executor,
	}
	cpus := math.Min(t.getCpus(), available_cpus)
	mem := t.getMem()
	task.Resources = []*mesos.Resource{
		mesos.ScalarResource("cpus", cpus),
		mesos.ScalarResource("mem", mem),
	}

	glog.Infof(" %s createTask :cpus : %#v, mem: %#v", tid, cpus, mem)
	return task
}

func (s *_MesosScheduler) stop() {
	if !s.started {
		return
	}
	glog.Info("stop scheduler")
	s.started = false
	s.isRegistered = false
	s.driver.Stop(false)
	s.driver = nil
}

func (d *_MesosScheduler) start() {
	Stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")
	Stderr := os.NewFile(uintptr(syscall.Stderr), "/dev/stderr")
	d.out_logger = d.start_logger(Stdout)
	d.err_logger = d.start_logger(Stderr)
}

func (d *_MesosScheduler) start_logger(*os.File) string {
	return ""
}

func (d *_MesosScheduler) runJob(done <-chan bool, finalRdd RDD, rn *JobFunc,
	additions []interface{},
	partitions []int, allowLocal bool) <-chan interface{} {
	return _runJob(d, done, finalRdd, rn, additions, partitions, allowLocal)
}
func (d *_MesosScheduler) check() {
}

func (d *_MesosScheduler) clear() {
	d.init_job()
}
func (s *_MesosScheduler) submitTasks(done <-chan bool, tasks []_Tasker) {
	if len(tasks) == 0 {
		return
	}
	taskRdd := tasks[0].getRdd()
	mem := taskRdd.getMem()
	if mem <= 0 {
		mem = s.mem
	}
	job := newSimpleJob(s, tasks, s.cpus, mem)
	jobId := job.getId()
	s.activeJobs[jobId] = job
	s.activeJobsQueue[jobId] = job
	s.jobTasks[jobId] = make(map[string]*mesos.TaskInfo)
	glog.Infof("Got job %d with %d tasks: %s",
		jobId, len(tasks), taskRdd)
	need_revive := s.started

	if !s.started {
		s.start_driver()
	}
	if need_revive {
		s.requestMoreResources()
	}
}
func (s *_MesosScheduler) requestMoreResources() {
	s.driver.ReviveOffers()
}
func (s *_MesosScheduler) jobFinished(_job interface{}) {
	job := _job.(Jobber)
	jobId := job.getId()
	glog.Infof("job %d finished", jobId)
	if _, ok := s.activeJobs[jobId]; ok {
		delete(s.activeJobs, jobId)
		delete(s.activeJobsQueue, jobId)
		for tid, _ := range s.jobTasks[jobId] {
			delete(s.taskIdToJobId, tid)
			delete(s.taskIdToSlaveId, tid)
		}
		delete(s.jobTasks, jobId)
		s.last_finish_time = time.Now()

		if len(s.activeJobs) == 0 {
			s.slaveTasks = make(map[string]int)
			s.slaveFailed = make(map[string]int)
		}
	}
}

func (s *_MesosScheduler) GetError(driver *mesos.SchedulerDriver, msg string) {
	fmt.Println(msg)
	glog.Fatal("GetError")
}

func (s *_MesosScheduler) ExecutorLost(driver *mesos.SchedulerDriver, executorId mesos.ExecutorID,
	slaveId mesos.SlaveID, id int) {
	glog.Fatal("ExecutorLost")
}

func (s *_MesosScheduler) start_driver() {
	fpath, _ := filepath.Abs(os.Args[0])
	name := "[gopark] " + fpath + " " + strings.Join(os.Args[1:], " ")
	if len(name) > 256 {
		name = name[:256] + "..."
	}
	framework := mesos.FrameworkInfo{
		Name: proto.String(name),
		User: proto.String(""), //TODO:GetUser
	}
	sch := &mesos.Scheduler{
		ResourceOffers: s.ResourceOffers,
		Registered:     s.Registered,
		StatusUpdate:   s.StatusUpdate,
		Reregistered: func(driver *mesos.SchedulerDriver, _ mesos.MasterInfo) {
			fmt.Println("Registered")
		},
		Disconnected: func(driver *mesos.SchedulerDriver) {
			fmt.Println("Disconnected")
		},
		OfferRescinded: func(driver *mesos.SchedulerDriver, _ mesos.OfferID) {
			fmt.Println("OfferRescinded")
		},
		Error:        s.GetError,
		ExecutorLost: s.ExecutorLost,
		SlaveLost: func(driver *mesos.SchedulerDriver, _ mesos.SlaveID) {
			fmt.Println("SlaveLost")
		},
		FrameworkMessage: func(driver *mesos.SchedulerDriver, _ mesos.ExecutorID, _ mesos.SlaveID, msg string) {
			fmt.Println(msg)
		},
	}
	s.driver = &mesos.SchedulerDriver{
		Master:    s.master,
		Framework: framework,
		Scheduler: sch,
	}
	s.driver.Init()
	s.driver.Start()
	glog.Infof("Mesos Scheudler driver started [%s]", s.master)
	s.started = true
	s.last_finish_time = time.Now()

	go func() {
		for s.started {
			now := time.Now()
			duration := now.Sub(s.last_finish_time)
			if len(s.activeJobs) == 0 && duration.Seconds() > MAX_IDLE_TIME {
				glog.Infof("stop mesos scheduler after %d seconds idle",
					duration.Seconds())
				glog.Flush()
				s.stop()
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()
}
