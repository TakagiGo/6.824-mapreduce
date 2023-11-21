package _Web_Crawler

import "github.com/google/uuid"

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
