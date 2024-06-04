/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package connector

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Templum/rabbitmq-connector/pkg/config"
	"github.com/Templum/rabbitmq-connector/pkg/rabbitmq"
	"github.com/Templum/rabbitmq-connector/pkg/types"
	"github.com/streadway/amqp"
)

// RabbitToOpenFaaS defines the basic interactions for the connector
type RabbitToOpenFaaS interface {
	Run() error
	Shutdown()
}

// New creates a new connector instance using the provided parameters & config to build it up
func New(manager rabbitmq.Manager, factory rabbitmq.Factory, invoker types.Invoker, conf *config.Controller) RabbitToOpenFaaS {
	return &Connector{
		client: invoker,

		factory:    factory,
		conManager: manager,
		conf:       conf,
	}
}

// Connector includes all relevant information that the connector needs to hold and maintain
type Connector struct {
	client types.Invoker

	factory    rabbitmq.Factory
	conManager rabbitmq.Manager
	conf       *config.Controller
	exchanges  []rabbitmq.ExchangeOrganizer
}

// Run starts the connector and creates a connection RabbitMQ. Further it implements the defined Topology.
// Also it adds a listener that handles connection failures.
func (c *Connector) Run() error {
	c.logJSON("info", "Started RabbitMQ <=> OpenFaaS Connector", nil)
	c.logJSON("info", "Will now establish connection", map[string]interface{}{
		"RabbitMQ URL": c.conf.RabbitSanitizedURL,
	})

	failureChan, conErr := c.conManager.Connect(c.conf.RabbitConnectionURL)
	if conErr != nil {
		return conErr
	}

	go c.HandleConnectionError(failureChan)

	genErr := c.generateExchangesFrom(c.conf.Topology)
	if genErr != nil {
		return genErr
	}

	for _, ex := range c.exchanges {
		err := ex.Start()
		if err != nil {
			return err
		}
	}

	go c.logHealthStatus()

	return nil
}

// HandleConnectionError listens for incoming connection errors. If it is recoverable it will attempt a self-heal.
// Otherwise it shutsdown the whole connector
func (c *Connector) HandleConnectionError(ch <-chan *amqp.Error) {
	err := <-ch
	c.logJSON("error", "RabbitMQ Connection failed", map[string]interface{}{
		"reason":  err.Reason,
		"code":    err.Code,
		"server":  err.Server,
		"recover": err.Recover,
	})

	if err.Recover {
		for _, ex := range c.exchanges {
			ex.Stop()
		}

		// Release old exchange refs to garbage collection
		c.exchanges = nil
		err := c.Run()
		if err != nil {
			c.logJSON("fatal", "Received critical error during restart, shutting down", map[string]interface{}{
				"error": err,
			})
			log.Panicf("Received critical error: %s during restart, shutting down", err)
		}
	} else {
		c.logJSON("fatal", "Received critical error, shutting down", map[string]interface{}{
			"error": err,
		})
		log.Panicf("Received critical error: %s, shutting down", err)
	}
}

// Shutdown is usually called during graceful shutdown. It stops all exchanges and finally closes the connection
// to RabbitMQ
func (c *Connector) Shutdown() {
	c.logJSON("info", "Shutdown RabbitMQ <=> OpenFaaS Connector", nil)

	// Loop over Exchanges to close
	for _, ex := range c.exchanges {
		ex.Stop()
	}

	// Close Connection
	c.conManager.Disconnect()
}

func (c *Connector) generateExchangesFrom(t types.Topology) error {
	// Do we want to use a connection per Exchange or continue with channels?
	c.factory.WithChanCreator(c.conManager).WithInvoker(c.client)

	for _, topology := range c.conf.Topology {
		tmp := types.Exchange(topology)
		exchange, buildErr := c.factory.WithExchange(&tmp).Build()

		if buildErr != nil {
			return buildErr
		}

		c.exchanges = append(c.exchanges, exchange)
	}

	return nil
}

// logHealthStatus periodically logs the health status of the connector
func (c *Connector) logHealthStatus() {
	ticker := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-ticker.C:
			c.logJSON("info", "Health status", map[string]interface{}{
				"exchanges": len(c.exchanges),
			})
		}
	}
}

// logJSON logs a message in JSON format
func (c *Connector) logJSON(level, message string, fields map[string]interface{}) {
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
