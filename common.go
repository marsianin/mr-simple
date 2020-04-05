package mr

import (
	"hash/fnv"
	"strconv"
)

// jobPhase indicates whether a task is scheduled as a
// MapFunc or ReduceFunc task.
type jobPhase string

const (
	mapPhase    jobPhase = "MapFunc"
	reducePhase          = "ReduceFunc"
)

//
// MapFunc functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by MapFunc.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mr-out." + strconv.Itoa(reduceTask)
}