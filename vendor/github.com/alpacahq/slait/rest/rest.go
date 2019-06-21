package rest

import (
	"errors"
	"fmt"
	"net/http/pprof"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/slait/cache"
	"github.com/alpacahq/slait/socket"
	"github.com/kataras/iris"
	"github.com/kataras/iris/core/handlerconv"
)

func (rest REST) Start() error {
	app := iris.New()

	app.HandleMany("GET HEAD", "/heartbeat", HeartbeatHandler)
	app.HandleMany("GET POST DELETE", "/topics", TopicsHandler)
	app.HandleMany("GET PUT DELETE", "/topics/{topic:string}", TopicHandler)
	app.HandleMany("GET PUT DELETE", "/topics/{topic:string}/{partition:string}", PartitionHandler)
	app.Any("/ws", iris.FromStd(socket.GetHandler().Serve))
	// profiling
	app.Any("/debug/pprof/{action:path}", Profiler())

	return app.Run(iris.Addr(":" + rest.Port))
}

type REST struct {
	Port string
}

type TopicsRequest struct {
	Topic      string
	Partitions []string
}

func Profiler() iris.Handler {
	indexHandler := handlerconv.FromStd(pprof.Index)
	cmdlineHandler := handlerconv.FromStd(pprof.Cmdline)
	profileHandler := handlerconv.FromStd(pprof.Profile)
	symbolHandler := handlerconv.FromStd(pprof.Symbol)
	goroutineHandler := handlerconv.FromStd(pprof.Handler("goroutine"))
	heapHandler := handlerconv.FromStd(pprof.Handler("heap"))
	threadcreateHandler := handlerconv.FromStd(pprof.Handler("threadcreate"))
	debugBlockHandler := handlerconv.FromStd(pprof.Handler("block"))

	return func(ctx iris.Context) {
		ctx.ContentType("text/html")
		actionPathParameter := ctx.Params().Get("action")
		if len(actionPathParameter) > 1 {
			if strings.Contains(actionPathParameter, "cmdline") {
				cmdlineHandler((ctx))
			} else if strings.Contains(actionPathParameter, "profile") {
				profileHandler(ctx)
			} else if strings.Contains(actionPathParameter, "symbol") {
				symbolHandler(ctx)
			} else if strings.Contains(actionPathParameter, "goroutine") {
				goroutineHandler(ctx)
			} else if strings.Contains(actionPathParameter, "heap") {
				heapHandler(ctx)
			} else if strings.Contains(actionPathParameter, "threadcreate") {
				threadcreateHandler(ctx)
			} else if strings.Contains(actionPathParameter, "debug/block") {
				debugBlockHandler(ctx)
			}
		} else {
			indexHandler(ctx)
		}
	}
}

// GET: get list of topics
// POST: create a new topic
// DELETE: delete all topics
func TopicsHandler(ctx iris.Context) {
	switch ctx.Method() {
	case "GET":
		keys := reflect.ValueOf(cache.Catalog()).MapKeys()
		topics := make([]string, len(keys))
		for i := 0; i < len(keys); i++ {
			topics[i] = keys[i].String()
		}
		respondWithJSON(ctx, topics, iris.StatusOK)
	case "POST":
		tReq := TopicsRequest{}
		if err := ctx.ReadJSON(&tReq); err != nil {
			respondWithError(ctx, err.Error(), iris.StatusBadRequest)
			return
		}
		if tReq.Topic == "" {
			respondWithError(ctx, "Topic is required", iris.StatusBadRequest)
			return
		}
		if err := cache.Add(tReq.Topic); err != nil {
			respondWithError(ctx, err.Error(), iris.StatusBadRequest)
			return
		}
		if len(tReq.Partitions) > 0 {
			for _, p := range tReq.Partitions {
				if err := cache.Update(tReq.Topic, p, cache.AddPartition); err != nil {
					respondWithError(ctx, err.Error(), iris.StatusBadRequest)
					return
				}
			}
		}
		respondWithJSON(ctx, nil, iris.StatusOK)
	case "DELETE":
		for topic := range cache.Catalog() {
			cache.Remove(topic)
		}
		respondWithJSON(ctx, nil, iris.StatusOK)
	}
}

// GET: list all the partition keys under this topic
// PUT: update the metadata of a topic
// DELETE: delete a topic
func TopicHandler(ctx iris.Context) {
	topic := ctx.Params().Get("topic")
	switch ctx.Method() {
	case "GET":
		pMap := cache.Catalog()[topic]
		partitions := make([]string, len(pMap))
		i := 0
		for partition := range pMap {
			partitions[i] = partition
			i++
		}
		respondWithJSON(ctx, partitions, iris.StatusOK)
	case "PUT":
		// TODO: implement metadata update
	case "DELETE":
		cache.Remove(topic)
		respondWithJSON(ctx, nil, iris.StatusOK)
	}
}

type PartitionRequestResponse struct {
	Data cache.Entries
}

// GET: query entries from a partition
// PUT: append new entries to a partition. a new partition is created if non-existent.
// DELETE: delete a partition along with its entries
func PartitionHandler(ctx iris.Context) {
	topic := ctx.Params().Get("topic")
	partition := ctx.Params().Get("partition")
	switch ctx.Method() {
	case "GET":
		params := ctx.Request().URL.Query()
		from, err := parseTimeString(params.Get("from"), "from")
		if err != nil {
			respondWithError(ctx, err.Error(), iris.StatusBadRequest)
			return
		}
		to, err := parseTimeString(params.Get("to"), "to")
		if err != nil {
			respondWithError(ctx, err.Error(), iris.StatusBadRequest)
			return
		}
		last, _ := strconv.ParseInt(params.Get("last"), 10, 32)
		respondWithJSON(
			ctx,
			PartitionRequestResponse{Data: cache.Get(topic, partition, from, to, int(last))},
			iris.StatusOK,
		)
	case "PUT":
		pReq := PartitionRequestResponse{}
		if err := ctx.ReadJSON(&pReq); err != nil {
			respondWithError(ctx, err.Error(), iris.StatusBadRequest)
			return
		}
		if len(pReq.Data) == 0 {
			respondWithError(ctx, "Data is required", iris.StatusBadRequest)
			return
		}
		entries := make(cache.Entries, len(pReq.Data))
		for i := 0; i < len(pReq.Data); i++ {
			entries[i] = &cache.Entry{
				Timestamp: pReq.Data[i].Timestamp,
				Data:      []byte(pReq.Data[i].Data),
			}
		}
		cache.Append(topic, partition, entries)
		respondWithJSON(ctx, nil, iris.StatusOK)
	case "DELETE":
		cache.Update(topic, partition, cache.RemovePartition)
		respondWithJSON(ctx, nil, iris.StatusOK)
	}
}

func respondWithError(ctx iris.Context, message string, code int) {
	respondWithJSON(ctx, map[string]string{"message": message}, code)
}

func respondWithJSON(ctx iris.Context, body interface{}, code int) {
	ctx.StatusCode(code)
	ctx.ContentType("application/json")
	ctx.JSON(body)
}

func parseTimeString(tStr, fieldName string) (*time.Time, error) {
	tPtr := &time.Time{}
	t, err := time.Parse(time.RFC3339, tStr)
	if err != nil {
		if tStr == "" {
			tPtr = nil
		} else {
			return nil, errors.New(
				fmt.Sprintf(
					"Invalid '%v' timestamp. Please format like: '2006-01-02T15:04:05-07:00'",
					fieldName))
		}
	} else {
		tPtr = &t
	}
	return tPtr, nil
}

func HeartbeatHandler(ctx iris.Context) {
	respondWithJSON(
		ctx,
		map[string]interface{}{"status": "alive"},
		iris.StatusOK,
	)
}
