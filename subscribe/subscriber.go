package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Subscriber allows subscription.
type Subscriber struct {
	closeChan chan struct{}
	conn      *amqp.Connection
	ch        *amqp.Channel
	cfg       struct {
		ConnectionString string
		Username         string
		Password         string
		ConsumeQueueName string
		//ProduceExchange         string
		//PubRouteKey             string
		ConsumerWorkers       int
		ConsumerPrefetchCount int
	}
	isClosed bool
}

// New return new subscriber
func New() *Subscriber {
	return &Subscriber{}
}

// Title returns events title.
func (sub *Subscriber) Title() string {
	return "Subscriber Rabbit"
}

// Start starts event module
func (sub *Subscriber) Start() {
	sub.closeChan = make(chan struct{})

	var err error
	sub.conn, err = amqp.Dial(fmt.Sprintf(sub.cfg.ConnectionString, sub.cfg.Username, sub.cfg.Password))
	if err != nil {
		logrus.WithError(err).Fatal("ampq.dial")
	}

	sub.ch, err = sub.conn.Channel()
	if err != nil {
		logrus.WithError(err).Fatal("ampq.chanel")
	}

	errChan := make(chan *amqp.Error)
	sub.ch.NotifyClose(errChan)
	go func() {
		err = <-errChan
		close(sub.closeChan)

		if err != nil {
			logrus.Debug(err)
		}

		logrus.Debug("NotifyClose")
	}()

	err = sub.ch.Qos(sub.cfg.ConsumerPrefetchCount, 0, false)
	if err != nil {
		logrus.Fatal(err)
	}

	msgs, err := sub.ch.Consume(
		"ConsumeQueueName", // queue
		"",                 // consumer
		false,              // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		logrus.Fatal(err)
	}

	for i := 0; i < sub.cfg.ConsumerWorkers; i++ {
		go func() {
			for m := range msgs {

				logrus.Info(m.Body)

				ackErr := m.Ack(false)
				if ackErr != nil {
					logrus.WithError(ackErr).Error("Ack")
				}
				//	nackErr := m.Nack(false, false)
			}
		}()
	}

	<-sub.closeChan

	if sub.isClosed {
		return
	}

	sub.Start()
}

// Stop stops event module
func (sub *Subscriber) Stop() {
	sub.isClosed = true

	if sub.ch != nil {
		err := sub.ch.Close()
		if err != nil {
			logrus.WithError(err).Error("chanel close error")
		}
	}

	if sub.conn != nil {
		err := sub.conn.Close()
		if err != nil {
			logrus.Error("connection close error")
		}
	}
}
