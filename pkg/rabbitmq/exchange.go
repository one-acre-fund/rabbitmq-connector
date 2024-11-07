package rabbitmq

import (
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

// OverallHealthMetrics tracks the overall health metrics of the connector
type OverallHealthMetrics struct {
	sync.Mutex
	TotalMessages         int
	SuccessfulInvocations int
	FailedInvocations     int
}

// Exchange contains all of the relevant units to handle communication with an exchange
type Exchange struct {
	channel ChannelConsumer
	client  types.Invoker

	definition *types.Exchange
	lock       sync.RWMutex

	healthMetrics  *OverallHealthMetrics
	prefetchCount  int
	prefetchSize   int
	prefetchGlobal bool
}

// MaxAttempts of retries that will be performed
const MaxAttempts = 3

// NewExchange creates a new exchange instance using the provided parameter
func NewExchange(channel ChannelConsumer, client types.Invoker, definition *types.Exchange, healthMetrics *OverallHealthMetrics, prefetchCount, prefetchSize int, prefetchGlobal bool) ExchangeOrganizer {
	return &Exchange{
		channel: channel,
		client:  client,

		definition:     definition,
		lock:           sync.RWMutex{},
		healthMetrics:  healthMetrics,
		prefetchCount:  prefetchCount,
		prefetchSize:   prefetchSize,
		prefetchGlobal: prefetchGlobal,
	}
}

// Start begins consuming deliveries from the exchange's queue.
// It also creates a listener for channel errors.
func (e *Exchange) Start() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	logJSON("info", "Starting exchange", map[string]interface{}{
		"exchange": e.definition.Name,
		"bypass":   e.definition.Bypass,
	})

	if e.definition.Bypass {
		logJSON("info", "Skipping processing for exchange because Bypass is set", map[string]interface{}{
			"exchange": e.definition.Name,
		})
		return nil
	}

	err := e.channel.Qos(
		e.prefetchCount,
		e.prefetchSize,
		e.prefetchGlobal,
	)
	if err != nil {
		return err
	}

	closeChannel := make(chan *amqp.Error)
	e.channel.NotifyClose(closeChannel)
	go e.handleChanFailure(closeChannel)

	deliveries, err := e.channel.Consume(e.definition.Queue, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	// go e.logHealthStatus()
	// go e.logOverallHealthStatus()

	go e.StartConsuming(deliveries)

	return nil
}

// Stop stops consuming messages
func (e *Exchange) Stop() {
	e.lock.Lock()
	defer e.lock.Unlock()

	_ = e.channel.Close()
}

func (e *Exchange) handleChanFailure(ch <-chan *amqp.Error) {
	err := <-ch
	logJSON("error", "Channel failure", map[string]interface{}{
		"exchange": e.definition.Name,
		"error":    err.Error(),
	})
}

// StartConsuming will consume deliveries from the provided channel and if the received delivery's routing key
// matches a topic we're interested in, it will invoke the respective function. If it doesn't match, it will reject the message.
func (e *Exchange) StartConsuming(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		e.healthMetrics.Lock()
		e.healthMetrics.TotalMessages++
		e.healthMetrics.Unlock()

		if e.isTopicOfInterest(delivery.RoutingKey) {
			bodyStr := strings.Replace(string(delivery.Body), "\n", "", -1)
			logJSON("info", "Received body", map[string]interface{}{
				"body": bodyStr,
			})
			go e.handleInvocation(delivery.RoutingKey, delivery)
		} else {
			logJSON("info", "Received message with uninteresting topic, rejecting", map[string]interface{}{
				"exchange": e.definition.Name,
				"topic":    delivery.RoutingKey,
			})

			for retry := 0; retry < MaxAttempts; retry++ {
				err := delivery.Reject(true)
				if err == nil {
					logJSON("info", "Successfully rejected delivery", map[string]interface{}{
						"deliveryTag": delivery.DeliveryTag,
					})
					return
				}
				logJSON("error", "Failed to reject delivery", map[string]interface{}{
					"deliveryTag": delivery.DeliveryTag,
					"attempt":     retry + 1,
					"error":       err.Error(),
				})
				time.Sleep(time.Duration(retry+1) * 250 * time.Millisecond)
			}

			logJSON("error", "Failed to reject delivery, aborting", map[string]interface{}{
				"deliveryTag": delivery.DeliveryTag,
			})
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
	err := e.client.Invoke(topic, types.NewInvocation(delivery))
	if err == nil {
		e.healthMetrics.Lock()
		e.healthMetrics.SuccessfulInvocations++
		e.healthMetrics.Unlock()

		for retry := 0; retry < MaxAttempts; retry++ {
			ackErr := delivery.Ack(false)
			if ackErr == nil {
				logJSON("info", "Successfully acknowledged delivery", map[string]interface{}{
					"deliveryTag": delivery.DeliveryTag,
				})
				return
			}
			logJSON("error", "Failed to acknowledge delivery", map[string]interface{}{
				"deliveryTag": delivery.DeliveryTag,
				"attempt":     retry + 1,
				"error":       ackErr.Error(),
			})
			time.Sleep(time.Duration(retry+1) * 250 * time.Millisecond)
		}

		logJSON("error", "Failed to acknowledge delivery, aborting", map[string]interface{}{
			"deliveryTag": delivery.DeliveryTag,
		})
	} else {
		e.healthMetrics.Lock()
		e.healthMetrics.FailedInvocations++
		e.healthMetrics.Unlock()

		for retry := 0; retry < MaxAttempts; retry++ {
			nackErr := delivery.Nack(false, true)
			if nackErr == nil {
				logJSON("info", "Successfully nacked delivery", map[string]interface{}{
					"deliveryTag": delivery.DeliveryTag,
				})
				return
			}
			logJSON("error", "Failed to nack delivery", map[string]interface{}{
				"deliveryTag": delivery.DeliveryTag,
				"attempt":     retry + 1,
				"error":       nackErr.Error(),
			})
			time.Sleep(time.Duration(retry+1) * 250 * time.Millisecond)
		}

		logJSON("error", "Failed to nack delivery, aborting", map[string]interface{}{
			"deliveryTag": delivery.DeliveryTag,
		})
	}
}

func (e *Exchange) logHealthStatus() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		status := map[string]interface{}{
			"@t":    time.Now().UTC().Format(time.RFC3339),
			"@m":    "Health check status",
			"@i":    "health-check",
			"@l":    "Info",
			"queue": e.definition.Queue,
			"exchange": map[string]interface{}{
				"name":       e.definition.Name,
				"durable":    e.definition.Durable,
				"autoDelete": e.definition.AutoDeleted,
			},
		}
		logJSON("info", "Health check status", status)
	}
}

func (e *Exchange) logOverallHealthStatus() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		e.healthMetrics.Lock()
		status := map[string]interface{}{
			"@t":                    time.Now().UTC().Format(time.RFC3339),
			"@m":                    "Overall health check status",
			"@i":                    "overall-health-check",
			"@l":                    "Info",
			"totalMessages":         e.healthMetrics.TotalMessages,
			"successfulInvocations": e.healthMetrics.SuccessfulInvocations,
			"failedInvocations":     e.healthMetrics.FailedInvocations,
		}
		e.healthMetrics.Unlock()

		logJSON("info", "Overall health check status", status)
	}
}
