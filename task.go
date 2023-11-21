package _Web_Crawler

import (
	"log"
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskStatus int

const (
	Undo TaskStatus = iota
	Working
	Done
)

type Task struct {
	taskType      TaskType
	taskId        int
	files         []string
	nReduceNumber int
}

func NewTask(taskType TaskType, taskId int, files []string, nReduceNumber int) *Task {
	return &Task{
		taskType,
		taskId,
		files,
		nReduceNumber,
	}
}

type TaskMetaData struct {
	task       *Task
	taskStatus TaskStatus
	startTime  time.Time
}

func NewTaskMetaData(task *Task) *TaskMetaData {
	return &TaskMetaData{
		task:       task,
		taskStatus: Undo,
	}
}

type TaskSet struct {
	MapTaskSet    map[int]*TaskMetaData
	ReduceTaskSet map[int]*TaskMetaData
}

func NewTaskSet() *TaskSet {
	return &TaskSet{
		MapTaskSet:    map[int]*TaskMetaData{},
		ReduceTaskSet: map[int]*TaskMetaData{},
	}
}
func (t *TaskSet) registerTask(task *Task) {
	taskMetaData := NewTaskMetaData(task)
	switch task.taskType {
	case MapTask:
		t.MapTaskSet[task.taskId] = taskMetaData
	case ReduceTask:
		t.ReduceTaskSet[task.taskId] = taskMetaData
	default:
		log.Panic("can't register Task for task %v", task)
	}
}
func (t *TaskSet) StartTask(task *Task) {
	var taskMap map[int]*TaskMetaData
	switch task.taskType {
	case MapTask:
		taskMap = t.MapTaskSet
	case ReduceTask:
		taskMap = t.ReduceTaskSet
	default:
		log.Panic("receive an invalid task type")
	}
	taskMap[task.taskId].taskStatus = Working
	taskMap[task.taskId].startTime = time.Now()
}

func (t *TaskSet) CompleteTask(task *Task) {
	var taskMap map[int]*TaskMetaData
	switch task.taskType {
	case MapTask:
		taskMap = t.MapTaskSet
	case ReduceTask:
		taskMap = t.ReduceTaskSet
	default:
		log.Panic("receive an invalid task type")
	}
	taskMap[task.taskId].taskStatus = Done
}

func (t *TaskSet) IsAllTaskComplete(tasktype TaskType) bool {
	var taskMap map[int]*TaskMetaData
	switch tasktype {
	case MapTask:
		taskMap = t.MapTaskSet
	case ReduceTask:
		taskMap = t.ReduceTaskSet
	}
	for _, taskMetaData := range taskMap {
		if taskMetaData.taskStatus != Done {
			return false
		}
	}
	return true
}

const TaskTimeOut = time.Duration(10)

func (t *TaskSet) TaskTimeOutDetector(tasktype TaskType) []*Task {
	var taskMap map[int]*TaskMetaData
	TaskTimeOutSet := []*Task{}
	switch tasktype {
	case MapTask:
		taskMap = t.MapTaskSet
	case ReduceTask:
		taskMap = t.ReduceTaskSet
	}
	for _, taskMetaData := range taskMap {
		if taskMetaData.taskStatus == Working {
			if TimeCheckInterval*time.Second < time.Since(taskMetaData.startTime) {
				TaskTimeOutSet = append(TaskTimeOutSet, taskMetaData.task)
			}
		}
	}
	return TaskTimeOutSet
}
