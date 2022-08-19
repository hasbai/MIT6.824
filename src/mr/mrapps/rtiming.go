package mrapps

//
// a MapReduce pseudo-application to test that workers
// execute reduce tasks in parallel.
//
// go build -buildmode=plugin rtiming.go
//

import (
	"6.824/models"
	"strconv"
)

type RTiming struct {
}

func (RTiming) Map(filename string, contents string) []models.KeyValue {
	var kva []models.KeyValue
	kva = append(kva, models.KeyValue{Key: "a", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "b", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "c", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "d", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "e", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "f", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "g", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "h", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "i", Value: "1"})
	kva = append(kva, models.KeyValue{Key: "j", Value: "1"})
	return kva
}

func (RTiming) Reduce(key string, values []string) string {
	n := nParallel("reduce")
	return strconv.Itoa(n)
}
