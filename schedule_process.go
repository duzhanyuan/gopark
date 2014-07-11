package gopark

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type _MultiProcessScheduler struct {
	*_LocalScheduler
}

func newMultiProcessScheduler(numRoutines int) Scheduler {
	sch := &_MultiProcessScheduler{
		_LocalScheduler: newLocalScheduler(numRoutines),
	}
	return sch
}

func (d *_MultiProcessScheduler) check() {
}

func (s *_MultiProcessScheduler) start() {
}

func (s *_MultiProcessScheduler) stop() {
}

func (d *_MultiProcessScheduler) runJob(done <-chan bool, finalRdd RDD,
	rn *JobFunc, additions []interface{},
	partitions []int, allowLocal bool) <-chan interface{} {
	return _runJob(d, done, finalRdd, rn, additions, partitions, allowLocal)
}

func run_proccess(addr string, wg *sync.WaitGroup) {
	if os.Getppid() != 1 { //parent proccess
		filePath, _ := filepath.Abs(os.Args[0])
		fmt.Println(addr)
		cmd := &exec.Cmd{}
		if runtime.GOOS == "windows" {
			cmd = exec.Command(filePath, "-process_child", addr, "-logtostderr", "0")
		} else {
			cmd = exec.Command("sh", "-c", filePath+" -process_child="+addr)
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Start()
		//TODO: time out
		cmd.Wait()
		wg.Done()
	}
}

func do_process_job(addr string) {
	var client *rpc.Client
	var err error
	i := 0
	for i < 4 {
		client, err = rpc.Dial("tcp", addr)
		if err == nil {
			break
		}
		i += 1
		time.Sleep(1000 * time.Millisecond)
	}
	if err != nil {
		glog.Fatal(err)
	}
	defer client.Close()
	getEnv := func(client *rpc.Client) map[string]interface{} {
		res := DataReq{}
		rep := DataEnv{}
		if err := client.Call("RPC_Task.GetEnv", &res, &rep); err != nil {
			glog.Fatal(err)
			return nil
		} else {
			return rep.Data
		}
	}
	environ := getEnv(client)
	_env.start(false, environ, false)

	getTask := func(client *rpc.Client) (_Tasker, bool) {
		res := ProcessResult{
			Action: "task",
		}
		rep := new(ProcessTask)
		if err := client.Call("RPC_Task.Get", &res, rep); err != nil {
			glog.Fatal(err)
			return nil, false
		} else {
			if rep.IsEnd {
				return nil, true
			} else {
				return rep.Task, false
			}
		}
	}

	doJob := func(client *rpc.Client) bool {
		task, doneAll := getTask(client)
		if !doneAll {
			done := make(chan bool)
			defer close(done)
			accumulators.Clear()
			var result interface{}
			var err error
			switch task.(type) {
			case *ResultTask:
				{
					result, err = task.run(done, 0)
				}
			case *ShuffleMapTask:
				{
					result, err = task.run(done, 0)
				}
			default:
				glog.Fatal("do not know task.")
			}
			accUpdate := accumulators.Values()
			res := ProcessResult{
				Action:  "result",
				Task:    task,
				Err:     err,
				Result:  result,
				Updates: accUpdate,
			}
			reply := new(ProcessTask)
			fmt.Println("RPC_Task.Get")
			if err = client.Call("RPC_Task.Get", &res, reply); err != nil {
				glog.Fatal(err)
			}
		}
		return doneAll
	}
	doneAll := doJob(client)
	for doneAll == false {
		doneAll = doJob(client)
	}
	fmt.Println("job all done")
}

func (s *_MultiProcessScheduler) submitTasks(done <-chan bool, tasks []_Tasker) {
	if len(tasks) == 0 {
		return
	} else {
		glog.Infof("submit tasks %s in MultiProcessScheduler[Num:%d]", tasks, s.numRoutines)
	}
	addr, r, listen := s.startRPCTask()
	defer listen.Close()
	go func() {
		defer close(r.task)
		for _, task := range tasks {
			r.task <- task
		}
	}()

	go func() {
		wg := &sync.WaitGroup{}
		for i := 0; i < s.numRoutines; i++ {
			wg.Add(1)
			go run_proccess(addr, wg)
		}
		wg.Wait()
	}()

	idx := 0
	for res := range r.result {
		s.taskEnded(res.Task, res.Err, res.Result, res.Updates)
		idx += 1
		if idx == len(tasks) {
			break
		}
	}
}

func (s *_MultiProcessScheduler) jobFinished(_job interface{}) {
}

func (s *_MultiProcessScheduler) startRPCTask() (string, *RPC_Task, net.Listener) {
	r := newRPC_Task()
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(r); err != nil {
		glog.Fatal(err)
	} else {
		if tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0"); err == nil {
			if lis, err := net.ListenTCP("tcp", tcpAddr); err == nil {
				addr := lis.Addr()
				if tcpAddr, ok := addr.(*net.TCPAddr); ok {
					port := tcpAddr.Port
					host, _ := os.Hostname()
					guide_addr := fmt.Sprintf("%s:%d", host, port)
					go func() {
						for {
							if conn, err := lis.Accept(); err != nil {
								//TODO:check err
								return
							} else {
								go rpcServer.ServeConn(conn)
							}
						}

					}()
					return guide_addr, r, lis
				}
			} else {
				glog.Fatal(err)
			}
		} else {
			glog.Fatal(err)
		}
	}
	return "", nil, nil
}

type ProcessResult struct {
	Action  string
	Task    _Tasker
	Err     error
	Result  interface{}
	Updates map[uint64]interface{}
}
type ProcessTask struct {
	Task  _Tasker
	IsEnd bool
}

type DataReq struct {
	Action string
}

type DataEnv struct {
	Data map[string]interface{}
}

type RPC_Task struct {
	task   chan _Tasker
	result chan *ProcessResult
}

func newRPC_Task() *RPC_Task {
	return &RPC_Task{
		task:   make(chan _Tasker),
		result: make(chan *ProcessResult),
	}
}

func _test_gob(data *ProcessTask, idx uint64) string {
	//for debug when gob can not encode task
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		glog.Fatal(err)
	}
	return fmt.Sprintf("RPC task[%d] send.", idx)
}

func (r *RPC_Task) GetEnv(action *DataReq, reply *DataEnv) error {
	workdir := _env.Get("WORKDIR", nil).([]string)
	_env.environ["SERVER_URI"] = fmt.Sprintf("file://%s", workdir[0])
	reply.Data = _env.environ
	glog.Info(_test_gob2(reply))
	return nil
}

func _test_gob2(data *DataEnv) string {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		glog.Fatal(err)
	}
	return "RPC env send."
}

func (r *RPC_Task) Get(result *ProcessResult, reply *ProcessTask) error {
	if result.Action == "task" {
		if task, ok := <-r.task; ok {
			reply.Task = task
			reply.IsEnd = false
			glog.Info(_test_gob(reply, task.getId()))
		} else {
			reply.IsEnd = true
		}
	} else if result.Action == "result" {
		//fmt.Println("RPC_Task.Get result")
		r.result <- result
	}

	return nil
}
