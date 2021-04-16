package main

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	nsqLookupds = []string{"127.0.0.1:4161"}
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
}

func NewConsumer(topic, channel, logPrefix string) *nsq.Consumer {
	consumerConfig := nsq.NewConfig()
	consumerConfig.MaxAttempts = 3
	consumerConfig.MaxInFlight = 1
	consumerConfig.MsgTimeout = 60 * time.Second
	consumerConfig.MaxBackoffDuration = 0 * time.Second

	// Create new consumer
	consumer, _ := nsq.NewConsumer(topic, channel, consumerConfig)
	consumer.SetLogger(log.New(os.Stderr, logPrefix, log.Ltime), nsq.LogLevelError)
	consumer.AddConcurrentHandlers(NewHandler(HandleTest), consumerConfig.MaxInFlight)

	// Open connection to NSQ
	if err := consumer.ConnectToNSQLookupds(nsqLookupds); err != nil {
		log.Fatalln(err)
	}

	return consumer
}

func NewHandler(handler func(m *nsq.Message) error) nsq.HandlerFunc {
	return func(message *nsq.Message) error {
		return handler(message)
	}
}
func HandleTest(m *nsq.Message) error {

	return errors.New("foo bar")
}
