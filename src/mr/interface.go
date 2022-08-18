package mr

import (
	"6.824/models"
	"6.824/mr/mrapps"
)

type MapFunc func(file string, contents string) []models.KeyValue

type ReduceFunc func(key string, values []string) string

type MapReduce interface {
	Map(file string, contents string) []models.KeyValue
	Reduce(key string, values []string) string
}

func CreateMapReduceApp(name string) MapReduce {
	switch name {
	case "wc":
		return mrapps.WC{}
	case "crash":
		return mrapps.Crash{}
	case "no crash":
		return mrapps.NoCrash{}
	case "indexer":
		return mrapps.Indexer{}
	case "job count":
		return mrapps.JobCount{}
	case "early exit":
		return mrapps.EarlyExit{}
	case "m timing":
		return mrapps.MTiming{}
	case "r timing":
		return mrapps.RTiming{}
	default:
		panic("name not supported")
	}
}
