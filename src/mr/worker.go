package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//var newTask TaskReponse
	//fmt.Println("worker started")
	var oldTask = TaskArgs{Type: TaskTypeNone}
	for {
		newTask := GetTask(oldTask)
		switch newTask.Type {
		case TaskTypeMap:
			//log.Printf("worker id map: %d", newTask.Id)
			f := newTask.Files[0]
			file, _ := os.Open(f)
			defer file.Close()
			content, _ := ioutil.ReadAll(file)
			intermediate := mapf(f, string(content))
			byReduceFiles := make(map[int][]KeyValue)
			for _, kv := range intermediate {
				idx := ihash(kv.Key) % newTask.NReduce
				byReduceFiles[idx] = append(byReduceFiles[idx], kv)
			}
			files := make([]string, newTask.NReduce)
			for reduceId, kvs := range byReduceFiles {
				filename := fmt.Sprintf("mr-%d-%d", newTask.Id, reduceId)
				// new task for this worker to perform
				ofile, _ := os.Create(filename)
				defer ofile.Close()
				enc := json.NewEncoder(ofile)
				for _, kv := range kvs {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				files[reduceId] = filename
			}
			oldTask = TaskArgs{Type: newTask.Type, Id: newTask.Id, Files: files}
		case TaskTypeReduce:
			//log.Printf("worker id reduce: %d", newTask.Id)
			intermediate := []KeyValue{}
			for _, filename := range newTask.Files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				defer file.Close()

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", newTask.Id)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			oldTask = TaskArgs{Type: newTask.Type, Id: newTask.Id, Files: []string{oname}}
			//log.Printf("Calling master to get new task as old task reduce is done %d", oldTask.Id)
		case TaskTypeSleep:
			time.Sleep(500 * time.Millisecond)
			oldTask = TaskArgs{Type: TaskTypeNone}
		case TaskTypeExit:
			return
		default:
			panic(fmt.Sprintf("unknown type: %v", newTask.Type))
		}
	}
}

func GetTask(reqArgs TaskArgs) TaskReponse {
	ret := TaskReponse{}
	ok := call("Coordinator.GetTask", &reqArgs, &ret)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return ret
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample(args ExampleArgs) {
//
//	// declare an argument structure.
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
