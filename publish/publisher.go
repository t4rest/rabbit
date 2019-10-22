package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitPub struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	cfg  struct {
		ConnectionString string
		Username         string
		Password         string
		//ConsumeQueueName string
		ProduceExchange string
		PubRouteKey     string
		//ConsumerWorkers       int
		//ConsumerPrefetchCount int
	}
	isConnected bool
}

// New creates new kafka connection
func New() (*RabbitPub, error) {

	rabbit := &RabbitPub{}

	err := rabbit.connect()
	if err != nil {
		return nil, err
	}

	return rabbit, nil
}

func (pub *RabbitPub) connect() error {
	var err error

	pub.conn, err = amqp.Dial(fmt.Sprintf(pub.cfg.ConnectionString, pub.cfg.Username, pub.cfg.Password))
	if err != nil {
		return err
	}

	pub.ch, err = pub.conn.Channel()
	if err != nil {
		pub.Close()
		return err
	}

	errChan := make(chan *amqp.Error)
	pub.ch.NotifyClose(errChan)

	pub.isConnected = true

	go func() {
		<-errChan
		pub.isConnected = false
	}()

	return nil
}

func (pub *RabbitPub) maybeReconnect() error {
	if pub.isConnected {
		return nil
	}

	return pub.connect()
}

// Publish publish event
func (pub *RabbitPub) Publish(ctx context.Context) error {
	err := pub.maybeReconnect()
	if err != nil {
		return err
	}

	messageID, err := uuid.NewV4()
	if err != nil {
		return err
	}

	test := make(map[string]string)
	test["test"] = "test"

	body, err := json.Marshal(test)
	if err != nil {
		return err
	}

	return pub.ch.Publish(
		pub.cfg.ProduceExchange, // exchange
		pub.cfg.PubRouteKey,     // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			CorrelationId: messageID.String(),
			Timestamp:     time.Now(),
			DeliveryMode:  amqp.Persistent,
			ContentType:   "application/json",
			Body:          body,
		})
}

// Close close connection
func (pub *RabbitPub) Close() {
	if pub.ch != nil {
		err := pub.ch.Close()
		if err != nil {
			logrus.WithError(err).Error("chanel close error")
		}
	}

	if pub.conn != nil {
		err := pub.conn.Close()
		if err != nil {
			logrus.WithError(err).Error("connection close error")
		}
	}
}
