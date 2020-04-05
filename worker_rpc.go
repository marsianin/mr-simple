package mr

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

//
// start a thread that listens for RPCs from master.go
//
func (w *MrWorker) startRPCServer(workerAddr string) {
	rpcServer := rpc.NewServer()
	rpcServer.Register(w)

	fmt.Printf("Start RPC server %s\n", workerAddr)

	// create tcp address
	tcpAddr, err := net.ResolveTCPAddr("tcp", workerAddr)
	if err != nil {
		log.Fatal(err)
	}
	l, e := net.ListenTCP("tcp", tcpAddr)
	if e != nil {
		log.Fatal("RunWorker: worker RPC ", workerAddr, " error: ", e)
	}

	w.rpcListener = l
	w.workerAddr = workerAddr

	call("MrMaster.RegisterACK", w.masterAddr, RegisterWorkerArgs{WorkerAddress:workerAddr}, struct{}{})

	for {
		select {
		case <-w.shutdown:
			break
		default:
		}

		conn, err := w.rpcListener.Accept()

		if err == nil {
			go func() {
				rpcServer.ServeConn(conn)
				conn.Close()
			}()
		} else {
			break
		}
	}

	w.rpcListener.Close()
}
