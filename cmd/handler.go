package main

import (
	"fmt"
	"github.com/cmazx/clickhouse/events"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"strconv"
	"time"
)

type RequestHandler struct {
	EventConsumer    *events.Consumer
	ClientSideScript string
}

func NewRequestHandler(script string) *RequestHandler {
	batchSize, err := strconv.Atoi(os.Getenv("APP_STATISTICS_BATCH_SIZE"))
	if err != nil {
		log.Fatal("STATISTICS_BATCH_SIZE env value must be an integer")
	}

	chProvider := events.NewStorage(os.Getenv("APP_CLICKHOUSE_URL"))
	chProvider.CreateDatabase()

	return &RequestHandler{
		EventConsumer:    events.NewConsumer(batchSize, chProvider),
		ClientSideScript: script,
	}
}

func (h *RequestHandler) HandleRequest(ctx *fasthttp.RequestCtx) {
	if !ctx.IsGet() {
		return
	}
	if string(ctx.Path()) == "/la.js" {
		ctx.Response.SetBodyString(h.ClientSideScript)
		ctx.Response.Header.SetContentType("text/javascript")
		return
	}

	if len(ctx.Path()) != 1 {
		return
	}

	tagsParams := ctx.QueryArgs().PeekMulti("tags[]")
	var tags []string
	for _, tag := range tagsParams {
		tags = append(tags, string(tag))
	}

	args := ctx.QueryArgs()
	h.EventConsumer.Enqueue(&events.Event{
		Gid:         string(args.Peek("gid")),
		Sid:         string(args.Peek("sid")),
		UtmSource:   string(args.Peek("utm_source")),
		UtmMedium:   string(args.Peek("utm_medium")),
		UtmCampaign: string(args.Peek("utm_campaign")),
		UtmTerm:     string(args.Peek("utm_term")),
		UtmContent:  string(args.Peek("utm_content")),
		EventValue:  string(args.Peek("event_value")),
		EventName:   string(args.Peek("event_name")),
		Tags:        tags,
		IP:          ctx.RemoteIP().String(),
		UserAgent:   string(ctx.UserAgent()),
		Time:        time.Now(),
	})
}

func (h *RequestHandler) StopEventConsumer() {
	fmt.Println("Stopping...")
	h.EventConsumer.Persist()
	h.EventConsumer.Stop()
	fmt.Println("Stopped.")
}

func (h *RequestHandler) StartEventConsumer() {
	h.EventConsumer.Consume()
}
