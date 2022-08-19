package mr

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	server           *http.Server
	tasks            []Task      // all tasks
	taskQueue        Stack[Task] // not assigned tasks
	workerTaskMap    sync.Map    // assigned tasks
	canReduce        *sync.Cond
	totalMapTasks    int32
	finishedMapTasks int32
}

// Your code here -- RPC handlers for the worker to call.

const timeout = time.Second * 10

// GetTask gives task to workers.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *Task) error {
	// finished task
	taskID, ok := c.workerTaskMap.Load(args.WorkerID)
	if ok {
		if c.tasks[taskID.(int)].Type == TaskTypeMap {
			atomic.AddInt32(&c.finishedMapTasks, 1)
		}
	}
	c.workerTaskMap.Delete(args.WorkerID)

	// get a task
	task := c.taskQueue.Pop()
	for task == emptyTask { // no tasks available, sleep
		if c.Done() {
			return nil
		}
		time.Sleep(time.Second)
		task = c.taskQueue.Pop()
	}

	*reply = task

	if task.Type == TaskTypeReduce && !c.allMapTasksDone() {
		reply.Code = TaskCodeWait
		c.taskQueue.Push(task)
		log.Printf("task %d is waiting", task.ID)
		return nil
		//c.canReduce.L.Lock()
		//for !c.allMapTasksDone() {
		//	log.Printf("task %d is waiting", task.ID)
		//	c.canReduce.Wait()
		//}
		//c.canReduce.L.Unlock()
		//c.canReduce.Broadcast()
	}

	c.workerTaskMap.Store(args.WorkerID, task.ID)
	time.AfterFunc(timeout, func() {
		c.timeout(args.WorkerID, task.ID)
	})

	return nil
}

// Done is called periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	var workerTaskMappingLength int
	c.workerTaskMap.Range(func(k, v any) bool {
		workerTaskMappingLength++
		return true
	})
	done := c.taskQueue.Len() == 0 && workerTaskMappingLength == 0
	if done {
		err := c.server.Close()
		if err != nil {
			panic(err)
		}
	}
	return done
}

func (c *Coordinator) timeout(workerID string, taskID int) {
	currentTaskID, ok := c.workerTaskMap.Load(workerID)
	if !ok || currentTaskID != taskID { // task finished
		//log.Printf("task %d finished, skip...", taskID)
		return
	}
	c.workerTaskMap.Delete(workerID)
	c.taskQueue.Push(c.tasks[taskID])
	log.Printf("task %d timeout, rescheduled", taskID)
	fmt.Println(c.taskQueue.array)
	c.workerTaskMap.Range(func(k, v any) bool {
		fmt.Println(k, v)
		return true
	})
}

func (c *Coordinator) allMapTasksDone() bool {
	log.Println(atomic.LoadInt32(&c.finishedMapTasks))
	return c.totalMapTasks == atomic.LoadInt32(&c.finishedMapTasks)
}

// MakeCoordinator creates a Coordinator.
// main.go coordinator calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.canReduce = sync.NewCond(&sync.Mutex{})
	c.tasks = generateTasks(nReduce)
	for _, task := range c.tasks {
		c.taskQueue.Push(task)
		if task.Type == TaskTypeMap {
			c.totalMapTasks++
		}
	}

	go c.serve()

	return &c
}
