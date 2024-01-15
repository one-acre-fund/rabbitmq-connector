/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package rabbitmq

import (
	"errors"
	"fmt"
	"log"

	"github.com/Templum/rabbitmq-connector/pkg/types"
	"github.com/streadway/amqp"
)

// Factory for building a Exchange
type Factory interface {
	WithInvoker(client types.Invoker) Factory
	WithChanCreator(creator ChannelCreator) Factory
	WithExchange(ex *types.Exchange) Factory
	Build() (ExchangeOrganizer, error)
}

// NewFactory creates a new instance with no defaults set.
func NewFactory() Factory {
	return &ExchangeFactory{}
}

// ExchangeFactory keeps tracks of all the build options provided to it during construction
type ExchangeFactory struct {
	creator  ChannelCreator
	client   types.Invoker
	exchange *types.Exchange
}

// WithChanCreator sets the channel creator that will be used
func (f *ExchangeFactory) WithChanCreator(creator ChannelCreator) Factory {
	f.creator = creator
	return f
}

// WithInvoker sets the invoker which will interact with OpenFaaS
func (f *ExchangeFactory) WithInvoker(client types.Invoker) Factory {
	f.client = client
	return f
}

// WithExchange sets the exchange definition and further ensures that the correct type is used
func (f *ExchangeFactory) WithExchange(ex *types.Exchange) Factory {
	log.Printf("Factory is configured for exchange %s", ex.Name)
	ex.EnsureCorrectType()
	f.exchange = ex
	return f
}

// Build uses the set values and builds a new exchange from them
func (f *ExchangeFactory) Build() (ExchangeOrganizer, error) {
	if f.creator == nil {
		return nil, errors.New("no channel creator was provided")
	}
	if f.client == nil {
		return nil, errors.New("no openfaas client was provided")
	}
	if f.exchange == nil {
		return nil, errors.New("no exchange configured")
	}

	channel, err := f.creator.Channel()
	if err != nil {
		return nil, err
	}

	topologyErr := declareTopology(channel, f.exchange)
	if topologyErr != nil {
		return nil, topologyErr
	}

	return NewExchange(channel, f.client, f.exchange), nil
}

func declareTopology(con RabbitChannel, ex *types.Exchange) error {
	if ex.Declare {
		err := con.ExchangeDeclare(ex.Name, ex.Type, ex.Durable, ex.AutoDeleted, false, false, amqp.Table{})
		if err != nil {
			log.Printf("Error declaring exchange %s: %v", ex.Name, err)
			return err
		}
		log.Printf("Successfully declared exchange %s of type %s { Durable: %t, Auto-Delete: %t }", ex.Name, ex.Type, ex.Durable, ex.AutoDeleted)
	}

	queueArgs := amqp.Table{}

	if ex.TTL > 0 {
		queueArgs["x-message-ttl"] = ex.TTL * 1000 // Convert TTL to milliseconds
		log.Printf("Set TTL for queue %s to %d milliseconds", ex.Queue, ex.TTL*1000)
	}
	if ex.DeadLetterExch != "" {
		queueArgs["x-dead-letter-exchange"] = ex.DeadLetterExch
		log.Printf("Set Dead Letter Exchange for queue %s to %s", ex.Queue, ex.DeadLetterExch)
	}

	_, declareErr := con.QueueDeclare(
		ex.Queue,
		ex.Durable,
		ex.AutoDeleted,
		false,
		false,
		queueArgs,
	)
	if declareErr != nil {
		log.Printf("Error declaring queue %s: %v", ex.Queue, declareErr)
		return declareErr
	}
	log.Printf("Successfully declared Queue %s", ex.Queue)

	for _, topic := range ex.Topics {
		bindErr := con.QueueBind(
			ex.Queue,
			topic,
			ex.Name,
			false,
			amqp.Table{},
		)

		if bindErr != nil {
			log.Printf("Error binding Queue %s to exchange %s with routing key %s: %v", ex.Queue, ex.Name, topic, bindErr)
			return bindErr
		}
		log.Printf("Successfully bound Queue %s to exchange %s with routing key %s", ex.Queue, ex.Name, topic)
	}

	return nil
}

// GenerateQueueName is responsible to generate a unique queue for the connector to use
// It follows the naming schema [EXCHANGE_NAME]_[TOPIC]
func GenerateQueueName(ex string, topic string) string {
	return fmt.Sprintf("%s_%s", ex, topic)
}
