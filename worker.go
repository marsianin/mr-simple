package mr

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)
import "log"

// MrWorker holds the state for a startRPCServer waiting for Task or Shutdown RPCs
type MrWorker struct {
	sync.Mutex

	masterAddr  string
	workerAddr  string
	name        string
	MapFunc     func(string, string) []KeyValue
	ReduceFunc  func(string, []string) string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	isBusy      bool // mutex
	rpcListener net.Listener
	shutdown    chan struct{}
}


// Tell the master worker exists and ready to work
func (w *MrWorker) register(masterAddr string) (workerAddress string, err error) {
	args := &RegisterWorkerArgs{}
	reply := &RegisterWorkerReply{}
	w.masterAddr = masterAddr

	ok := call("MrMaster.Register", w.masterAddr, args, reply)
	if ok == false || reply.Success == false {
		fmt.Printf("Register: RPC %s register error\n", w.masterAddr)
		return "", errors.New("Register: RPC %s register error.")
	}

	fmt.Printf("Worker with Addr %v has been successfully started\n", reply.WorkerAddress)

	return reply.WorkerAddress, nil
}

func (w *MrWorker) DoTask(args *DoTaskArgs, reply *DoTaskReply) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		w.name, args.Phase, args.TaskNumber, args.File, args.NumberOtherPhase)

	switch args.Phase {
	case mapPhase:
		doMap(args.JobName, args.TaskNumber, args.File, args.NumberOtherPhase, w.MapFunc)
	case reducePhase:
		doReduce(args.JobName, args.TaskNumber, args.NumberOtherPhase, w.ReduceFunc)
	}

	fmt.Printf("%s: %v task #%d done\n", w.name, args.Phase, args.TaskNumber)

	return nil
}

func (w *MrWorker) Shutdown(args *ShutdownArgs, reply *ShutdownReply) error {
	fmt.Printf("Worker %s Shutdown\n", w.workerAddr)
	w.rpcListener.Close()
	return nil
}

func NewWorker(masterAddress string, MapFunc func(string, string) []KeyValue, ReduceFunc func(string, []string) string) {
	// Your worker implementation here.
	//CallAskTask()
	w := new(MrWorker)
	w.shutdown = make(chan struct{})
	w.MapFunc = MapFunc
	w.ReduceFunc = ReduceFunc

	workerAddress, err := w.register(masterAddress)

	if err != nil {
		log.Fatal("RunWorker: error: ", err)
	}

	w.name = workerAddress
	w.startRPCServer(workerAddress)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string)  {
	NewWorker(":"+strconv.Itoa(MASTER_PORT), mapf, reducef)
}