package mr

import (
	"os"
	"path/filepath"
	"sync"
	"time"
)

var emptyTask Task

type Task struct {
	ID       int
	Code     TaskCode
	Type     TaskType
	FilePath string
	NReduce  int
	timer    *time.Timer
}

type TaskType string

const (
	TaskTypeMap    = "map"
	TaskTypeReduce = "reduce"
)

type TaskCode int

const (
	TaskCodeExit TaskCode = iota
	TaskCodeSuccess
	TaskCodeWait
)

type Stack[T any] struct {
	sync.Mutex
	array []T
}

func (s *Stack[T]) Push(t T) {
	s.Lock()
	defer s.Unlock()
	if s.IsEmpty() {
		s.array = make([]T, 0, 10)
	}
	s.array = append(s.array, t)
}

func (s *Stack[T]) Top() T {
	length := len(s.array)
	if length == 0 {
		var t T
		return t
	}
	return s.array[length-1]
}

func (s *Stack[T]) Pop() T {
	if s.IsEmpty() {
		var t T
		return t
	}
	var t T
	s.Lock()
	defer s.Unlock()
	s.array, t = s.array[:s.Len()-1], s.array[s.Len()-1]
	return t
}

func (s *Stack[T]) IsEmpty() bool {
	return s.Len() == 0
}

func (s *Stack[T]) Len() int {
	length := len(s.array)
	return length
}

func generateTasks(nReduce int) []Task {
	tasks := make([]Task, 0, 10)
	i := 0
	for i < nReduce {
		tasks = append(tasks, Task{
			ID:      i,
			Type:    TaskTypeReduce,
			NReduce: nReduce,
		})
		i++
	}
	err := filepath.WalkDir(
		"data",
		func(path string, file os.DirEntry, err error) error {
			if file.IsDir() {
				return nil
			}
			tasks = append(tasks, Task{
				ID:       i,
				Type:     TaskTypeMap,
				FilePath: path,
				NReduce:  nReduce,
			})
			i++
			return nil
		},
	)
	if err != nil {
		panic(err)
	}

	return tasks
}
