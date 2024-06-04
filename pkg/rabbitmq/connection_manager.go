/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package rabbitmq

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// LogJSON is a helper function to log messages in JSON format
func LogJSON(level, message, application string, fields map[string]interface{}) {
	logData := map[string]interface{}{
		"@t":          time.Now().UTC().Format(time.RFC3339),
		"@m":          message,
		"@i":          level,
		"@l":          "Info",
		"Application": application,
	}
	for k, v := range fields {
		logData[k] = v
	}
	logJSON, _ := json.Marshal(logData)
	log.Println(string(logJSON))
}

// Connector is a high-level interface for connection-related methods
type Connector interface {
	Connect(connectionURL string) (<-chan *amqp.Error, error)
	Disconnect()
}

// Manager is an interface that combines the relevant methods to connect to Rabbit MQ
// And create a new channel on an existing connection.
type Manager interface {
	Connector
	ChannelCreator
}

// ConnectionManager is tasked with managing the connection to Rabbit MQ
type ConnectionManager struct {
	con     RBConnection
	lock    sync.RWMutex
	dialer  RBDialer
	tlsConf *tls.Config
}

// NewConnectionManager creates a new instance using the provided dialer
func NewConnectionManager(dialer RBDialer, conf *tls.Config) Manager {
	return &ConnectionManager{
		lock:    sync.RWMutex{},
		con:     nil,
		dialer:  dialer,
		tlsConf: conf,
	}
}

// Connect uses the provided connection urls and tries up to 3 times to establish a connection.
// The retries are performed exponentially starting with 2s. It also creates a listener for close notifications.
func (m *ConnectionManager) Connect(connectionURL string) (<-chan *amqp.Error, error) {
	for attempt := 0; attempt < 3; attempt++ {

		con, err := m.dial(connectionURL)

		if err == nil {
			LogJSON("info", "Successfully established connection to Rabbit MQ Cluster", "rabbitmq-connector", nil)
			m.lock.Lock()
			m.con = con
			m.lock.Unlock()

			closeChannel := make(chan *amqp.Error)
			con.NotifyClose(closeChannel)
			return closeChannel, nil
		}

		LogJSON("error", "Failed to establish connection", "rabbitmq-connector", map[string]interface{}{
			"error":   err.Error(),
			"attempt": attempt + 1,
		})
		time.Sleep(time.Duration(2*attempt+1) * time.Second)
	}

	return nil, errors.New("could not establish connection to Rabbit MQ Cluster")
}

// Disconnect closes the connection and frees up the reference
func (m *ConnectionManager) Disconnect() {
	m.lock.Lock()
	defer m.lock.Unlock()

	err := m.con.Close()
	if err != nil {
		LogJSON("error", "Received error during closing connection", "rabbitmq-connector", map[string]interface{}{
			"error": err.Error(),
		})
	}

	m.con = nil
}

// Channel creates a new Rabbit MQ channel on the existing connection
func (m *ConnectionManager) Channel() (RabbitChannel, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.con.Channel()
}

func (m *ConnectionManager) dial(connectionURL string) (RBConnection, error) {
	if m.tlsConf == nil {
		return m.dialer.Dial(connectionURL)
	}

	return m.dialer.DialTLS(connectionURL, m.tlsConf)
}
