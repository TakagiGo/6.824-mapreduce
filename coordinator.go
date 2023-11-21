package _Web_Crawler

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

type TaskStage int

const (
	mapStage TaskStage = iota
	reduceStage
	doneStage
)

type coordinator struct {
	ID            uuid.UUID
	nReduceNumber int
	nMapNumber    int
	fileNames     []string
	taskChannel   chan *Task
	taskSet       TaskSet
	mutex         sync.Mutex
	stage         TaskStage
}

func (c *coordinator) RegisterMapTask() {
	for index, fileName := range c.fileNames {
		task := NewTask(MapTask, index, []string{fileName}, c.nReduceNumber)
		c.taskSet.registerTask(task)
		go func() {
			c.taskChannel <- task
		}()
	}
	log.Printf("[coordinator]Register MapTask successfully!")
}

const IntermediateNameTemplate string = "mr-tmp-%d-%d"

func (c *coordinator) RegisterReduceTask() {
	for i := 0; i < c.nReduceNumber; i++ {
		task := NewTask(ReduceTask, i, []string{}, c.nReduceNumber)
		c.taskSet.registerTask(task)
		for j := 0; j < c.nMapNumber; j++ {
			fileName := fmt.Sprintf(IntermediateNameTemplate, j, i)
			task.files = append(task.files, fileName)
		}
		go func() {
			c.taskChannel <- task
		}()
	}
	log.Printf("[coordinator]Register MapTask successfully!")
}

func (c *coordinator) CheckStageToComplete() bool {
	switch c.stage {
	case mapStage:
		return c.taskSet.IsAllTaskComplete(MapTask)
	case reduceStage:
		return c.taskSet.IsAllTaskComplete(ReduceTask)
	case doneStage:
		return true
	default:
		log.Panic("can't Check invalid Stage")
		return false
	}
}
func (c *coordinator) toNextStage() {
	switch c.stage {
	case mapStage:
		c.stage = reduceStage
		c.RegisterReduceTask()
	case reduceStage:
		c.stage = doneStage
	case doneStage:
	}
}

func (c *coordinator) FetchTask(args *FetchArgs, reply *FetchReply) error {
	log.Printf("[coordinator]worker node %d fetch for request.Msg is %s", args.nodeId, args.msg)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	select {
	case task := <-c.taskChannel:
		msg := "Fetch task successfully"
		reply.task = task
		reply.msg = msg
		c.taskSet.StartTask(task)
	default:
		if c.CheckStageToComplete() {
			c.toNextStage()
		}
		if c.stage == doneStage {
			msg := "all task finished.Exited gracefully"
			reply.task = &Task{taskType: ExitTask}
			reply.msg = msg
		} else {
			msg := "no available task to be fetched"
			reply.task = &Task{taskType: WaitTask}
			reply.msg = msg
		}
	}
	return nil
}

func (c *coordinator) SubmitTask(args *SubmitArgs, reply *SubmitReply) error {
	log.Printf("[coordinator]worker node %d submit a task. Msg is %s", args.nodeId, args.msg)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.taskSet.CompleteTask(args.task)
	msg := "submit task successfully."
	reply.msg = msg
	return nil
}

const TimeCheckInterval = time.Duration(2)

func (c *coordinator) TimeOutDetector() {
	var taskOutTimeSet []*Task
	for {
		switch c.stage {
		case mapStage:
			taskOutTimeSet = c.taskSet.TaskTimeOutDetector(MapTask)
		case reduceStage:
			taskOutTimeSet = c.taskSet.TaskTimeOutDetector(ReduceTask)
		case doneStage:
			return
		}
		for _, timeOutTask := range taskOutTimeSet {
			c.taskSet.registerTask(timeOutTask)
			go func(task *Task) {
				c.taskChannel <- task
			}(timeOutTask)
		}
		time.Sleep(TimeCheckInterval * time.Second)
	}
}

const WaitInterval = time.Duration(4)

func (c *coordinator) Done() bool {
	for {
		c.mutex.Lock()
		switch c.stage {
		case doneStage:
			c.mutex.Unlock()
			return true
		default:
			c.mutex.Unlock()
			time.Sleep(WaitInterval * time.Second)

		}
	}

}
