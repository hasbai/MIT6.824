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
	taskQueue        Stack[Task] // not assigned tasks
	workerTaskMap    sync.Map    // assigned tasks
	totalMapTasks    int32
	finishedMapTasks int32
}

// Your code here -- RPC handlers for the worker to call.

const timeout = time.Second * 10

// GetTask gives task to workers.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *Task) error {
	// finished task
	finishedTask, ok := c.loadTaskMap(args.WorkerID)
	if ok {
		if finishedTask.Type == TaskTypeMap {
			atomic.AddInt32(&c.finishedMapTasks, 1)
		}
		finishedTask.timer.Stop()
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
	}

	reply.Code = TaskCodeSuccess
	c.workerTaskMap.Store(args.WorkerID, &task)
	task.timer = time.AfterFunc(timeout, func() {
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
	currentTask, ok := c.loadTaskMap(workerID)
	if !ok || currentTask.ID != taskID { // task finished
		//log.Printf("task %d finished, skip...", taskID)
		return
	}
	c.workerTaskMap.Delete(workerID)
	c.taskQueue.Push(*currentTask)
	log.Printf("task %d timeout, rescheduled", taskID)
	fmt.Println(c.taskQueue.array)
}

func (c *Coordinator) allMapTasksDone() bool {
	log.Println("map task finished num:", atomic.LoadInt32(&c.finishedMapTasks))
	return c.totalMapTasks == atomic.LoadInt32(&c.finishedMapTasks)
}

// MakeCoordinator creates a Coordinator.
// main.go coordinator calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	tasks := generateTasks(nReduce)
	for _, task := range tasks {
		c.taskQueue.Push(task)
		if task.Type == TaskTypeMap {
			c.totalMapTasks++
		}
	}

	go c.serve()

	return &c
}

func (c *Coordinator) loadTaskMap(workerID string) (*Task, bool) {
	task, ok := c.workerTaskMap.Load(workerID)
	if !ok {
		return nil, false
	}
	return task.(*Task), true
}
