package mr

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)
import "net"

const MASTER_PORT = 5555
const START_WORKER_PORT = 10000

type MrMasterWorker struct {
	WorkerAddr string
}

type MrMaster struct {
	sync.Mutex

	masterAddr        string
	done              chan bool
	workers           map[string]MrMasterWorker
	jobName           string
	files             []string
	nReduce           int
	rpcListener       net.Listener
	registerChannel   chan MrMasterWorker
	shutdown          chan struct{}
	workerAddressIncr int
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *MrMaster) Done() bool {
	ret := false

	select {
	case ret = <-m.done:
		break
	case <-time.After(1 * time.Second):
	}

	return ret
}

func (m *MrMaster) run(files []string, scheduleFunc func(phase jobPhase), finishFunc func()) {
	scheduleFunc(mapPhase)
	scheduleFunc(reducePhase)
	//doMerge(m.jobName, m.nReduce)
	finishFunc()

	fmt.Printf("MapFunc/ReduceFunc task completed\n")
	m.done <- true
}

func (m *MrMaster) stopWorkers() {
	m.Lock()
	defer m.Unlock()
	fmt.Printf("Workers to be stopped %v", m.workers)
	for _, w := range m.workers {
		ok := call("MrWorker.Shutdown", w.WorkerAddr, struct{}{}, struct{}{})
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		}
	}
}

//
// create a MrMaster.
//
func MakeMaster(files []string, nReduce int) *MrMaster {
	m := new(MrMaster)
	m.files = files
	m.nReduce = nReduce
	m.done = make(chan bool)
	m.shutdown = make(chan struct{})
	m.registerChannel = make(chan MrMasterWorker)
	m.workerAddressIncr = START_WORKER_PORT
	m.startRPCServer(":"+strconv.Itoa(MASTER_PORT))
	m.jobName = "MIT-6.824"
	m.workers = make(map[string]MrMasterWorker, 1000)

	go m.run(files, m.schedule, func() {
		m.stopWorkers()
		m.stopRPCServer()
	})

	return m
}
