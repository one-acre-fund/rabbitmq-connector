/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Templum/rabbitmq-connector/pkg/types"
	"github.com/streadway/amqp"
)

// Factory for building an Exchange
type Factory interface {
	WithInvoker(client types.Invoker) Factory
	WithChanCreator(creator ChannelCreator) Factory
	WithExchange(ex *types.Exchange) Factory
	WithHealthMetrics(metrics *OverallHealthMetrics) Factory
	WithQoS(prefetchCount, prefetchSize int, global bool) Factory
	Build() (ExchangeOrganizer, error)
}

// NewFactory creates a new instance with no defaults set.
func NewFactory() Factory {
	return &ExchangeFactory{}
}

// ExchangeFactory keeps tracks of all the build options provided to it during construction
type ExchangeFactory struct {
	creator       ChannelCreator
	client        types.Invoker
	exchange      *types.Exchange
	healthMetrics *OverallHealthMetrics
	prefetchCount int
	prefetchSize  int
	global        bool
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
	logJSON("info", "Factory configured for exchange", map[string]interface{}{
		"exchange": ex.Name,
	})
	ex.EnsureCorrectType()
	f.exchange = ex
	return f
}

// WithHealthMetrics sets the health metrics to be used
func (f *ExchangeFactory) WithHealthMetrics(metrics *OverallHealthMetrics) Factory {
	f.healthMetrics = metrics
	return f
}

// WithQoS sets the QoS settings to be used
func (f *ExchangeFactory) WithQoS(prefetchCount, prefetchSize int, global bool) Factory {
	f.prefetchCount = prefetchCount
	f.prefetchSize = prefetchSize
	f.global = global
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
	if f.healthMetrics == nil {
		return nil, errors.New("no health metrics provided")
	}

	channel, err := f.creator.Channel()
	if err != nil {
		return nil, err
	}

	topologyErr := declareTopology(channel, f.exchange)
	if topologyErr != nil {
		return nil, topologyErr
	}

	// Apply QoS settings
	err = channel.Qos(f.prefetchCount, f.prefetchSize, f.global)
	if err != nil {
		logJSON("error", "Error applying QoS settings", map[string]interface{}{
			"prefetch_count": f.prefetchCount,
			"prefetch_size":  f.prefetchSize,
			"global":         f.global,
			"error":          err,
		})
		return nil, err
	}

	return NewExchange(channel, f.client, f.exchange, f.healthMetrics, f.prefetchCount, f.prefetchSize, f.global), nil
}

func declareTopology(con RabbitChannel, ex *types.Exchange) error {
	if ex.Declare {
		err := con.ExchangeDeclare(ex.Name, ex.Type, ex.Durable, ex.AutoDeleted, false, false, amqp.Table{})
		if err != nil {
			logJSON("error", "Error declaring exchange", map[string]interface{}{
				"exchange": ex.Name,
				"error":    err,
			})
			return err
		}
		logJSON("info", "Successfully declared exchange", map[string]interface{}{
			"exchange":   ex.Name,
			"type":       ex.Type,
			"durable":    ex.Durable,
			"autoDelete": ex.AutoDeleted,
		})
	}

	queueArgs := amqp.Table{}

	if ex.TTL > 0 {
		queueArgs["x-message-ttl"] = ex.TTL
		logJSON("info", "Set TTL for queue", map[string]interface{}{
			"queue": ex.Queue,
			"ttl":   ex.TTL,
		})
	}
	logJSON("info", "Dead Letter Exchange", map[string]interface{}{
		"deadLetterExchange": ex.DLE,
	})
	logJSON("info", "Exchange Configuration", map[string]interface{}{
		"exchangeConfig": ex,
	})

	if ex.DLE != "" {
		queueArgs["x-dead-letter-exchange"] = ex.DLE
		logJSON("info", "Set Dead Letter Exchange for queue", map[string]interface{}{
			"queue":              ex.Queue,
			"deadLetterExchange": ex.DLE,
		})
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
		logJSON("error", "Error declaring queue", map[string]interface{}{
			"queue": ex.Queue,
			"error": declareErr,
		})
		return declareErr
	}
	logJSON("info", "Successfully declared Queue", map[string]interface{}{
		"queue": ex.Queue,
	})

	for _, topic := range ex.Topics {
		bindErr := con.QueueBind(
			ex.Queue,
			topic,
			ex.Name,
			false,
			amqp.Table{},
		)

		if bindErr != nil {
			logJSON("error", "Error binding Queue to exchange with routing key", map[string]interface{}{
				"queue":      ex.Queue,
				"exchange":   ex.Name,
				"routingKey": topic,
				"error":      bindErr,
			})
			return bindErr
		}
		logJSON("info", "Successfully bound Queue to exchange with routing key", map[string]interface{}{
			"queue":      ex.Queue,
			"exchange":   ex.Name,
			"routingKey": topic,
		})
	}

	return nil
}

// GenerateQueueName is responsible to generate a unique queue for the connector to use
// It follows the naming schema [EXCHANGE_NAME]_[TOPIC]
func GenerateQueueName(ex string, topic string) string {
	return fmt.Sprintf("%s_%s", ex, topic)
}

func logJSON(level, message string, fields map[string]interface{}) {
	logEntry := make(map[string]interface{})
	logEntry["@t"] = time.Now().UTC().Format(time.RFC3339)
	logEntry["@m"] = message
	logEntry["@l"] = level
	logEntry["Application"] = "rabbitmq-connector"
	if fields != nil {
		for k, v := range fields {
			logEntry[k] = v
		}
	}

	logData, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("Failed to marshal log entry: %v", err)
		return
	}
	log.Println(string(logData))
}
