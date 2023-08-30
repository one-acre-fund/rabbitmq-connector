/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package rabbitmq

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Templum/rabbitmq-connector/pkg/types"
	"github.com/streadway/amqp"
)

// Starter defines something that can be started
type Starter interface {
	Start() error
}

// Stopper defines something that can be stopped
type Stopper interface {
	Stop()
}

// ExchangeOrganizer combines the ability to start & stop exchanges
type ExchangeOrganizer interface {
	Starter
	Stopper
}

// Exchange contains all of the relevant units to handle communication with an exchange
type Exchange struct {
	channel ChannelConsumer
	client  types.Invoker

	definition *types.Exchange
	lock       sync.RWMutex
}

// MaxAttempts of retries that will be performed
const MaxAttempts = 3

// NewExchange creates a new exchange instance using the provided parameter
func NewExchange(channel ChannelConsumer, client types.Invoker, definition *types.Exchange) ExchangeOrganizer {
	return &Exchange{
		channel: channel,
		client:  client,

		definition: definition,
		lock:       sync.RWMutex{},
	}
}

// Start begins consuming deliveries from the exchange's queue.
// It also creates a listener for channel errors.
func (e *Exchange) Start() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	closeChannel := make(chan *amqp.Error)
	e.channel.NotifyClose(closeChannel)
	go e.handleChanFailure(closeChannel)

	deliveries, err := e.channel.Consume(e.definition.Queue, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	// We consume messages from the single queue associated with the exchange.
	// The topics differentiation is handled within the consuming function.
	go e.StartConsuming(deliveries)

	return nil
}

// Stop s consuming messages
func (e *Exchange) Stop() {
	e.lock.Lock()
	defer e.lock.Unlock()

	// We ignore the issue since this method is usually called after connection failure.
	_ = e.channel.Close()
}

func (e *Exchange) handleChanFailure(ch <-chan *amqp.Error) {
	err := <-ch
	log.Printf("Received following error %s on channel for exchange %s", err, e.definition.Name)
}

// StartConsuming will consume deliveries from the provided channel and if the received delivery's routing key
// matches a topic we're interested in, it will invoke the respective function. If it doesn't match, it will reject the message.
func (e *Exchange) StartConsuming(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		if e.isTopicOfInterest(delivery.RoutingKey) {
			bodyStr := strings.Replace(string(delivery.Body), "\n", "", -1)
			log.Printf("Received body %s", bodyStr)
			go e.handleInvocation(delivery.RoutingKey, delivery)
		} else {
			log.Printf("Received message with topic %s for exchange %s, but it did not match any of the subscribed topics. Will reject it.", delivery.RoutingKey, e.definition.Name)

			for retry := 0; retry < MaxAttempts; retry++ {
				err := delivery.Reject(true)
				if err == nil {
					return
				}

				log.Printf("Failed to reject delivery %d due to %s. Attempt %d/3", delivery.DeliveryTag, err, retry+1)
				time.Sleep(time.Duration(retry+1*250) * time.Millisecond)
			}

			log.Printf("Failed to reject delivery %d, will abort reject now", delivery.DeliveryTag)
		}
	}
}

// Helper function to check if a routing key (topic) is of interest to this exchange
func (e *Exchange) isTopicOfInterest(topic string) bool {
	for _, t := range e.definition.Topics {
		if t == topic {
			return true
		}
	}
	return false
}

func (e *Exchange) handleInvocation(topic string, delivery amqp.Delivery) {
	// Call Function via Client
	err := e.client.Invoke(topic, types.NewInvocation(delivery))
	if err == nil {
		for retry := 0; retry < MaxAttempts; retry++ {
			ackErr := delivery.Ack(false)
			if ackErr == nil {
				return
			}

			log.Printf("Failed to acknowledge delivery %d due to %s. Attempt %d/3", delivery.DeliveryTag, ackErr, retry+1)
			time.Sleep(time.Duration(retry+1*250) * time.Millisecond)
		}

		log.Printf("Failed to acknowledge delivery %d, will abort ack now", delivery.DeliveryTag)
	} else {
		for retry := 0; retry < MaxAttempts; retry++ {
			nackErr := delivery.Nack(false, true)
			if nackErr == nil {
				return
			}

			log.Printf("Failed to nack delivery %d due to %s. Attempt %d/3", delivery.DeliveryTag, nackErr, retry+1)
			time.Sleep(time.Duration(retry+1*250) * time.Millisecond)
		}

		log.Printf("Failed to nack delivery %d, will abort nack now", delivery.DeliveryTag)
	}

}
