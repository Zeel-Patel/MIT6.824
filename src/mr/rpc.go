package mr

import "os"
import "strconv"

const (
	TaskTypeNone   = "TaskTypeNone"
	TaskTypeMap    = "TaskTypeMap"
	TaskTypeReduce = "TaskTypeReduce"
	TaskTypeSleep  = "TaskTypeSleep"
	TaskTypeExit   = "TaskTypeExit"
)

type TaskRequest struct {
	Type  string
	Id    int
	Files []string
}

type TaskReponse struct {
	Type    string
	Id      int
	Files   []string
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
