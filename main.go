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
	consumerConfig.MaxInFlight = 2
	consumerConfig.DefaultRequeueDelay = 10 * time.Second
	consumerConfig.MaxRequeueDelay = 15 * time.Second
	consumerConfig.BackoffStrategy = &ourBackoffStrategy{}

	// Create new consumer
	consumer, _ := nsq.NewConsumer(topic, channel, consumerConfig)
	consumer.SetLogger(log.New(os.Stderr, logPrefix, log.Ltime), nsq.LogLevelWarning)
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
	log.Println("consumed message for attempt ", m.Attempts)
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

type ourBackoffStrategy struct{}

func (o *ourBackoffStrategy) Calculate(attempt int) time.Duration {
	return time.Duration(20) * time.Second
}
