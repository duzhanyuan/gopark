package gopark

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"compress/gzip"
	"encoding/gob"
	"github.com/golang/glog"
	"github.com/xiexiao/gopark/mesos"
	"io/ioutil"
)

func do_executor_job(addr string) {
	driver := mesos.ExecutorDriver{
		Executor: &mesos.Executor{
			Registered: func(
				driver *mesos.ExecutorDriver,
				executor mesos.ExecutorInfo,
				framework mesos.FrameworkInfo,
				slave mesos.SlaveInfo) {
				var environ map[string]interface{}
				dec := gob.NewDecoder(bytes.NewBuffer(executor.Data))
				if err := dec.Decode(&environ); err != nil {
					glog.Fatal(err)
				}
				_env.start(false, environ, false)
			},

			LaunchTask: func(driver *mesos.ExecutorDriver, taskInfo mesos.TaskInfo) {
				taskId := taskInfo.TaskId
				if err := driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_RUNNING),
					Message: proto.String("task is running!"),
				}); err != nil {
					glog.Fatal(err)
				}

				reader, err := gzip.NewReader(bytes.NewBuffer(taskInfo.Data))
				if err != nil {
					glog.Fatal(err)
				}
				defer reader.Close()
				datas, err := ioutil.ReadAll(reader)
				if err != nil {
					glog.Fatal(err)
				}
				jobTask := &SimpleJobTask{}
				dec := gob.NewDecoder(bytes.NewBuffer(datas))
				if err := dec.Decode(jobTask); err != nil {
					glog.Fatal(err)
				}
				task := jobTask.Task
				accumulators.Clear()
				//TODO:try it 3 timess
				done := make(chan bool)
				defer close(done)
				result, err := task.run(done, 0)
				accUpdate := accumulators.Values()
				jobResult := &SimpleJobResult{
					Err:     err,
					Result:  result,
					Updates: accUpdate,
				}
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.Encode(jobResult); err != nil {
					glog.Fatal(err)
				}
				//TODO: when things is to big
				//TODO: when is url for shuffletak

				if err := driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_FINISHED),
					Data:    buf.Bytes(),
					Message: proto.String("task is done!"),
				}); err != nil {
					glog.Fatal(err)
				}
				glog.Infof("task[%s] is done!", taskId)
			},
		},
	}

	driver.Init()
	defer driver.Destroy()
	driver.Run()
}
