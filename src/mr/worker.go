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

	var oldTask = TaskRequest{Type: TaskTypeNone}
	for {
		newTask := GetTask(oldTask)
		switch newTask.Type {
		case TaskTypeMap:
			f := newTask.Files[0]
			reduceFiles := createIntermediateFiles(newTask.Id, f, mapf, newTask.NReduce)
			oldTask = TaskRequest{Type: newTask.Type, Id: newTask.Id, Files: reduceFiles}
		case TaskTypeReduce:
			outputFile := createFinalOutputFromIntermediateFile(newTask.Id, newTask.Files, reducef)
			oldTask = TaskRequest{Type: newTask.Type, Id: newTask.Id, Files: []string{outputFile}}
		case TaskTypeSleep:
			time.Sleep(500 * time.Millisecond)
			oldTask = TaskRequest{Type: TaskTypeNone}
		case TaskTypeExit:
			return
		default:
			panic(fmt.Sprintf("unknown type: %v", newTask.Type))
		}
	}
}

// Take input file and map function.
// Return R intermediate files containing key value pairs generated by map function.
func createIntermediateFiles(mapTaskId int, inputFile string, mapf func(string, string) []KeyValue, R int) []string {
	file, _ := os.Open(inputFile)
	defer file.Close()
	fileContent, _ := ioutil.ReadAll(file)
	kvList := mapf(inputFile, string(fileContent))

	// Map key value pairs to R using the ihash function
	kvListWithR := make(map[int][]KeyValue)
	for _, kv := range kvList {
		idx := ihash(kv.Key) % R
		kvListWithR[idx] = append(kvListWithR[idx], kv)
	}
	intermediateFiles := make([]string, R)
	for reduceId, kvs := range kvListWithR {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskId, reduceId)
		ofile, _ := os.Create(filename)
		defer ofile.Close()
		encoder := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}
		}
		intermediateFiles[reduceId] = filename
	}
	return intermediateFiles
}

// Take intermediate files and reduce function.
// Return output file.
func createFinalOutputFromIntermediateFile(reduceTaskId int, intermediateFiles []string, reducef func(string, []string) string) string {
	kvList := []KeyValue{}

	// generate key value pairs from intermediate files
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
	}
	sort.Sort(ByKey(kvList))

	outputFileName := fmt.Sprintf("mr-out-%d", reduceTaskId)
	outputFile, _ := os.Create(outputFileName)
	defer outputFile.Close()

	// call reduce function for values with same key
	i := 0
	for i < len(kvList) {
		j := i + 1
		for j < len(kvList) && kvList[j].Key == kvList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvList[k].Value)
		}
		output := reducef(kvList[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", kvList[i].Key, output)

		i = j
	}
	return outputFileName
}

func GetTask(reqArgs TaskRequest) TaskReponse {
	ret := TaskReponse{}
	ok := call("Coordinator.GetTask", &reqArgs, &ret)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return ret
}

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
