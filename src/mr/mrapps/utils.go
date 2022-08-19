package mrapps

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

func nParallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := getGoRoutineID()
	filename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(filename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	files, err := filepath.Glob(fmt.Sprintf("mr-worker-%s-*", phase))

	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range files {
		var xPid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xPid)
		if n == 1 && err == nil { // xPid is alive.
			ret++
		}
	}

	time.Sleep(1 * time.Second)

	err = os.Remove(filename)
	if err != nil {
		log.Println(err)
	}

	return ret
}

func getGoRoutineID() int {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return int(n)
}
