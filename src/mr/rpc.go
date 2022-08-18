package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Add your RPC definitions here.

type GetTaskArgs struct {
	WorkerID string
}

// Cook up a unique-ish UNIX-domain socket name for the coordinator.
// Can use the current directory since Windows support UNIX-domain sockets.
func coordinatorSock() string {
	return "824.sock"
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) serve() {
	socketName := coordinatorSock()
	_ = os.Remove(socketName)
	listener, e := net.Listen("unix", socketName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("listening on unix://%s", socketName)

	rpcServer := &rpc.Server{}
	err := rpcServer.Register(c)
	if err != nil {
		panic(err)
	}

	server := http.Server{Handler: rpcServer}
	c.server = &server
	err = server.Serve(listener)
	if err != nil {
		log.Println(err)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	socketName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", socketName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err != nil {
		fmt.Println(err)
	}
	return err == nil
}
