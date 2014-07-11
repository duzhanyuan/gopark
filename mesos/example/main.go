package main

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/xiexiao/gopark/mesos"
	"os"
	"path/filepath"
	"strconv"
)

type Data struct {
	Str string
}

func init() {
	gob.Register(&Data{})
}

func runExecutor() {
	driver := mesos.ExecutorDriver{
		Executor: &mesos.Executor{
			Registered: func(
				driver *mesos.ExecutorDriver,
				executor mesos.ExecutorInfo,
				framework mesos.FrameworkInfo,
				slave mesos.SlaveInfo) {
				fmt.Println("Executor registered!")
			},

			LaunchTask: func(driver *mesos.ExecutorDriver, taskInfo mesos.TaskInfo) {
				fmt.Println("Launch task!")
				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_RUNNING),
					Message: proto.String("Go task is running!"),
				})

				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_FINISHED),
					Message: proto.String("Go task is done!"),
				})
			},
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Run()
}

func main() {
	taskLimit := 1000
	taskId := 0
	exit := make(chan bool)
	localExecutor, _ := executorPath()

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	executorUri := flag.String("executor-uri", localExecutor, "URI of executor executable")
	flag.Parse()

	executable := false
	executor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
		Command: &mesos.CommandInfo{
			Value: proto.String("./example_executor"),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{Value: executorUri,
					Executable: &executable},
			},
		},
		Name:   proto.String("Test Executor (Go)"),
		Source: proto.String("go_test"),
	}
	str1 := "abcd"
	fmt.Println(str1)
	i := 0
	str := ""
	for i < 100000 {
		str = str + str1
		i = i + 1
	}

	driver := mesos.SchedulerDriver{
		Master: *master,
		Framework: mesos.FrameworkInfo{
			Name: proto.String("GoFramework"),
			User: proto.String(""),
		},

		Scheduler: &mesos.Scheduler{
			ResourceOffers: func(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
				for _, offer := range offers {
					taskId++
					tt := &Data{str}
					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					if err := enc.Encode(tt); err != nil {
						panic(err)
					}
					datas := buf.Bytes()

					fmt.Printf("Launching task: %d data:%d\n", taskId, len(datas))

					tasks := []*mesos.TaskInfo{
						&mesos.TaskInfo{
							Name: proto.String("go-task"),
							TaskId: &mesos.TaskID{
								Value: proto.String("go-task-" + strconv.Itoa(taskId)),
							},
							SlaveId:  offer.SlaveId,
							Executor: executor,
							Data:     datas,
							Resources: []*mesos.Resource{
								mesos.ScalarResource("cpus", 1),
								mesos.ScalarResource("mem", 128),
							},
						},
					}

					driver.LaunchTasks(offer.Id, tasks)
				}
			},

			StatusUpdate: func(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
				fmt.Println("Received task status: " + *status.Message)

				if *status.State == mesos.TaskState_TASK_FINISHED {
					taskLimit--
					if taskLimit <= 0 {
						exit <- true
					}
				}
			},
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Start()
	<-exit
	driver.Stop(false)
}

func executorPath() (string, error) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return "", err
	}

	path := dir + "/example_executor"
	return path, nil
}
