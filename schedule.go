package mr

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (MapFunc or ReduceFunc).
func (m *MrMaster) schedule(phase jobPhase) {
	var nTasks int
	var nInputOrOutput int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		nTasks = len(m.files)
		nInputOrOutput = m.nReduce
	case reducePhase:
		nTasks = m.nReduce
		nInputOrOutput = len(m.files)
	}

	fmt.Printf("Task(%s): %v, %v\n", phase, nTasks, nInputOrOutput)
	var wg sync.WaitGroup

	for i := 0 ; i < nTasks; i++ {
		wg.Add(1)

		go func(phase jobPhase, iTask int, numOtherPhase int) {
			var worker MrMasterWorker
			for {
				worker = <-m.registerChannel
				var args DoTaskArgs
				args.JobName = m.jobName
				args.Phase = phase
				args.TaskNumber = iTask
				args.NumberOtherPhase = numOtherPhase

				if phase == mapPhase {
					args.File = m.files[iTask]
				}

				reply := struct{}{}

				ok := call("MrWorker.DoTask", worker.WorkerAddr, &args, &reply)

				if ok {
					wg.Done()
					m.registerChannel <- worker
					return
				}
			}


		} (phase, i, nInputOrOutput)
	}

	wg.Wait()
}