package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	id        int
	file      string
	startTime time.Time
	done      bool
}

type ReduceTask struct {
	id        int
	files     []string
	startTime time.Time
	done      bool
}

type Coordinator struct {
	mutex        sync.Mutex
	mapTasks     []MapTask
	reduceTasks  []ReduceTask
	mapRemain    int
	reduceRemain int
}

func (c *Coordinator) GetTask(request *TaskRequest, response *TaskReponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// request contains the details of the last map/reduce task the worker executed
	switch request.Type {
	case TaskTypeMap:
		if !c.mapTasks[request.Id].done {
			c.mapTasks[request.Id].done = true
			for reduceId, file := range request.Files {
				if len(file) > 0 {
					c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, file)
				}
			}
			c.mapRemain--
		}
	case TaskTypeReduce:
		if !c.reduceTasks[request.Id].done {
			c.reduceTasks[request.Id].done = true
			c.reduceRemain--
		}
	}

	// Assign map or reduce task to worker
	now := time.Now()
	timeoutAgo := now.Add(-10 * time.Second)
	if c.mapRemain > 0 {
		for idx := range c.mapTasks {
			task := &c.mapTasks[idx]
			if !task.done && task.startTime.Before(timeoutAgo) {
				response.Type = TaskTypeMap
				response.Id = task.id
				response.Files = []string{task.file}
				response.NReduce = len(c.reduceTasks)

				task.startTime = now
				return nil
			}
		}
		response.Type = TaskTypeSleep
	} else if c.reduceRemain > 0 {
		for idx := range c.reduceTasks {
			task := &c.reduceTasks[idx]
			if !task.done && task.startTime.Before(timeoutAgo) {
				response.Type = TaskTypeReduce
				response.Id = task.id
				response.Files = task.files

				task.startTime = now
				return nil
			}
		}
		response.Type = TaskTypeSleep
	} else {
		response.Type = TaskTypeExit
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapRemain == 0 && c.reduceRemain == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:     make([]MapTask, len(files)),
		reduceTasks:  make([]ReduceTask, nReduce),
		mapRemain:    len(files),
		reduceRemain: nReduce,
	}

	// Your code here.

	for i, f := range files {
		c.mapTasks[i] = MapTask{id: i, file: f, done: false}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{id: i, done: false}
	}
	c.server()
	return &c
}
