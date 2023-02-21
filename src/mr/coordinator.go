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
	id    int
	file  string
	start time.Time
	done  bool
}

type ReduceTask struct {
	id    int
	files []string
	start time.Time
	done  bool
}

type Coordinator struct {
	// Your definitions here.
	mutex        sync.Mutex
	mapTasks     []MapTask
	reduceTasks  []ReduceTask
	mapRemain    int
	reduceRemain int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	fmt.Println("receeived call example")
//	fmt.Println(args.X)
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) GetTask(args *TaskArgs, res *TaskReponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.Type {
	case TaskTypeMap:
		//print(c.mapTasks[args.Id].done)
		if !c.mapTasks[args.Id].done {
			c.mapTasks[args.Id].done = true
			for reduceId, file := range args.Files {
				if len(file) > 0 {
					c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, file)
				}
			}
			c.mapRemain--
			//log.Printf("Remaining map: %d", c.mapRemain)

		}
	case TaskTypeReduce:
		if !c.reduceTasks[args.Id].done {
			c.reduceTasks[args.Id].done = true
			c.reduceRemain--
			//log.Printf("Remaining reduce: %d", c.reduceRemain)
		}
	}

	//log.Printf("Remaining map: %d, reduce: %d\n", c.mapRemain, c.reduceRemain)

	now := time.Now()
	timeoutAgo := now.Add(-100 * time.Second)
	if c.mapRemain > 0 {
		for idx := range c.mapTasks {
			t := &c.mapTasks[idx]
			if !t.done && t.start.Before(timeoutAgo) {
				res.Type = TaskTypeMap
				res.Id = t.id
				res.Files = []string{t.file}
				res.NReduce = len(c.reduceTasks)

				t.start = now
				//log.Printf("Assigned task map %d", res.Id)
				return nil
			}
		}
		res.Type = TaskTypeSleep
	} else if c.reduceRemain > 0 {
		for idx := range c.reduceTasks {
			t := &c.reduceTasks[idx]
			if !t.done && t.start.Before(timeoutAgo) {
				res.Type = TaskTypeReduce
				res.Id = t.id
				res.Files = t.files

				t.start = now
				//log.Printf("Assigned task reduce %d", res.Id)
				return nil
			}
		}
		res.Type = TaskTypeSleep
	} else {
		res.Type = TaskTypeExit
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
