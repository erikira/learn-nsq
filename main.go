package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	nsqLookupds = []string{"127.0.0.1:4161"}
	consumers   = []*nsq.Consumer{}
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)

	consumer := NewConsumer("eri_test", "Channel", "eri_test")
	consumers = append(consumers, consumer)
	log.Println("Consumer service running...")
	// subscribe to SIGINT signals
	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, os.Interrupt)
	<-stopChan // wait for SIGINT
	StopConsumers()
}

// NewConsumer create new consumer which connected to nsqd
func NewConsumer(topic, channel, logPrefix string) *nsq.Consumer {
	consumerConfig := nsq.NewConfig()
	consumerConfig.MaxAttempts = 3
	consumerConfig.MaxInFlight = 1
	consumerConfig.MsgTimeout = 60 * time.Second
	consumerConfig.MaxBackoffDuration = 0 * time.Second

	// Create new consumer
	consumer, _ := nsq.NewConsumer(topic, channel, consumerConfig)
	consumer.SetLogger(log.New(os.Stderr, logPrefix, log.Ltime), nsq.LogLevelError)
	consumer.AddConcurrentHandlers(NewHandler(HandleSaveOrder), consumerConfig.MaxInFlight)

	// Open connection to NSQ
	if err := consumer.ConnectToNSQLookupds(nsqLookupds); err != nil {
		log.Fatalln(err)
	}

	return consumer
}

// NewHandler wrapper to satisfy nsq.HandlerFunc
func NewHandler(handler func(m *nsq.Message) error) nsq.HandlerFunc {
	return func(message *nsq.Message) error {
		return handler(message)
	}
}

// HandleSaveOrder receive and process the message
func HandleSaveOrder(m *nsq.Message) error {

	return errors.New("foo bar")
}

// StopConsumers to stop all consumers
func StopConsumers() {
	for c := range consumers {
		if consumers[c] != nil {
			consumers[c].Stop()
		}
	}
}
