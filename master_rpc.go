package mr

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (m *MrMaster) Register(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.Lock()
	defer m.Unlock()

	workerAddr := ":"+strconv.Itoa(m.workerAddressIncr)
	m.workerAddressIncr++
	registeredWorker := MrMasterWorker{WorkerAddr: workerAddr}
	m.workers[workerAddr] = registeredWorker

	reply.Success = true
	reply.WorkerAddress = workerAddr

	return nil
}

func (m *MrMaster) RegisterACK(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.Lock()
	defer m.Unlock()
	registeredWorker := m.workers[args.WorkerAddress]
	m.workers[args.WorkerAddress] = registeredWorker

	reply.Success = true

	go func() {
		m.registerChannel <- registeredWorker
	}()

	return nil
}

func (m *MrMaster) Shutdown(_, _ *struct{}) error {
	fmt.Printf("Worker %s Shutdown\n", m.masterAddr)
	m.rpcListener.Close()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *MrMaster) startRPCServer(masterAddr string) {
	rpcServer := rpc.NewServer()
	rpcServer.Register(m)

	tcpAddr, err := net.ResolveTCPAddr("tcp", masterAddr)
	if err != nil {
		log.Fatal(err)
	}

	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to listen on given port: %s", err))
	}

	m.rpcListener = l
	m.masterAddr = masterAddr

	go func() {
		select {
		case <-m.shutdown:
			break
		default:
		}

		for {
			conn, err := m.rpcListener.Accept()

			if err == nil {
				go func() {
					rpcServer.ServeConn(conn)
					conn.Close()
				}()
			} else {
				break
			}
		}
	}()
}

// stopRPCServer stops the master RPC server.
// This must be done through an RPC to avoid race conditions between the RPC
// server thread and the current thread.
func (m *MrMaster) stopRPCServer() {
	var reply ShutdownReply
	ok := call("MrMaster.Shutdown", m.masterAddr, new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Shutdown: RPC %s error\n", m.masterAddr)
	}
}