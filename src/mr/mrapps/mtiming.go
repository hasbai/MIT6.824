package mrapps

//
// a MapReduce pseudo-application to test that workers
// execute map tasks in parallel.
//
// go build -buildmode=plugin mtiming.go
//

import (
	"6.824/models"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MTiming struct {
}

func (MTiming) Map(filename string, contents string) []models.KeyValue {
	t0 := time.Now()
	ts := float64(t0.Unix()) + (float64(t0.Nanosecond()) / 1000000000.0)
	pid := getGoRoutineID()

	n := nParallel("map")

	var kvs []models.KeyValue
	kvs = append(kvs, models.KeyValue{
		Key:   fmt.Sprintf("times-%v", pid),
		Value: fmt.Sprintf("%.1f", ts)})
	kvs = append(kvs, models.KeyValue{
		Key:   fmt.Sprintf("parallel-%v", pid),
		Value: fmt.Sprintf("%d", n)})
	return kvs
}

func (MTiming) Reduce(key string, values []string) string {
	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
