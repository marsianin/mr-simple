package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

//
// RPC definitions.
//

// Add your RPC definitions here.

// Shutdown worker
type ShutdownArgs struct {}

type ShutdownReply struct {
	Success bool
}

// Register worker
type RegisterWorkerArgs struct {
	WorkerAddress string
}

type RegisterWorkerReply struct {
	WorkerAddress string
	Success bool
}

// Task
type DoTaskArgs struct {
	JobName string
	Phase jobPhase
	TaskNumber int
	NumberOtherPhase int
	File string
}

type DoTaskReply struct {
	Filename string
	Success bool
}

//
// send an RPC request to the master->worker or worker->master,
// wait for the response. usually returns true.
// returns false if something goes wrong.
func call(rpcname, addr string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}