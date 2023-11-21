package _Web_Crawler

import (
	"github.com/google/uuid"
	"os"
	"strconv"
)

type FetchArgs struct {
	nodeId uuid.UUID
	msg    string
}

type FetchReply struct {
	task *Task
	msg  string
}
type SubmitArgs struct {
	nodeId uuid.UUID
	task   *Task
	msg    string
}

type SubmitReply struct {
	msg string
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
