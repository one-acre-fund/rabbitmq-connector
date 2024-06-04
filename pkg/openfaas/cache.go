/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package openfaas

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// TopicMap defines a interface for a topic map
type TopicMap interface {
	GetCachedValues(name string) []string
	Refresh(update map[string][]string)
}

// TopicFunctionCache contains a map of of topics to functions
type TopicFunctionCache struct {
	topicMap map[string][]string
	lock     sync.RWMutex
}

// NewTopicFunctionCache return a new instance
func NewTopicFunctionCache() *TopicFunctionCache {
	return &TopicFunctionCache{
		topicMap: make(map[string][]string),
		lock:     sync.RWMutex{},
	}
}

// GetCachedValues reads the cached functions for a given topic
func (m *TopicFunctionCache) GetCachedValues(name string) []string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	var functions []string
	for topic, function := range m.topicMap {
		if topic == name {
			functions = function
			break
		}
	}

	m.logJSON("info", "GetCachedValues", map[string]interface{}{
		"topic":     name,
		"functions": functions,
	})
	return functions
}

// Refresh updates the existing cache with new values while ensuring no read conflicts
func (m *TopicFunctionCache) Refresh(update map[string][]string) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.logJSON("info", "Updating cache", map[string]interface{}{
		"entries": len(update),
	})
	m.topicMap = update
	m.logJSON("info", "Cache refreshed", map[string]interface{}{
		"cache": m.topicMap,
	})
}

// logJSON logs a message in JSON format
func (m *TopicFunctionCache) logJSON(level, message string, fields map[string]interface{}) {
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
