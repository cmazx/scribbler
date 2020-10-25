package main

import (
	"bufio"
	"fmt"
	"github.com/getsentry/sentry-go"
	"github.com/joho/godotenv"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func loadEnv() {
	if _, ok := os.LookupEnv("ENV"); ok {
		fmt.Println("Load .env")
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error on .env file load")
		}
	}
}

func initApp() {
	_ = os.Setenv("TZ", os.Getenv("APP_TIMEZONE"))
}

func initSentry() {
	err := sentry.Init(sentry.ClientOptions{
		Dsn:   os.Getenv("SENTRY_DSN"),
		Debug: true,
	})
	if err != nil {
		log.Fatalf("sentry.CreateDatabase: %s", err)
	}
}

func loadScript() string {
	f, err := os.OpenFile("./la.js", os.O_RDONLY, 0755)
	if err != nil {
		log.Fatalf("Script file open err: %s", err)
	}

	r4 := bufio.NewReader(f)
	b, err := ioutil.ReadAll(r4)
	if err != nil {
		log.Fatalf("Script file read err: %s", err)
	}

	return string(b)
}

func main() {
	loadEnv()
	initSentry()
	defer sentry.Flush(2 * time.Second)
	initApp()

	requestHandler := NewRequestHandler(loadScript())

	handleOSSignals(requestHandler)
	go requestHandler.StartEventConsumer()
	defer requestHandler.StopEventConsumer()

	//Старт сервера
	srv := &fasthttp.Server{
		Handler: requestHandler.HandleRequest,
	}
	fmt.Println("handling requests on :8080")
	if err := srv.ListenAndServe(":8080"); err != nil {
		panic(err)
	}
}

func handleOSSignals(handler *RequestHandler) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for sig := range c {
			fmt.Println(sig)
			handler.StopEventConsumer()
		}
	}()
}
