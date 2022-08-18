package main

import (
	"6.824/mr"
	"os"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		return
	}
	switch os.Args[1] {
	case "sequential":
		mr.Sequential()
	case "coordinator":
		m := mr.MakeCoordinator(10)
		for m.Done() == false {
			time.Sleep(time.Second)
		}
		time.Sleep(time.Second)
	case "worker":
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ { // start multiple workers.
			wg.Add(1)
			go func() {
				mr.Worker("wc")
				wg.Done()
			}()
		}
		wg.Wait()
	default:
		return
	}
}
