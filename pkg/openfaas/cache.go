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
	GetCachedFilter(name string) string
	Refresh(topicMap map[string][]string)
	RefreshFilters(filterMap map[string]string)
}

// TopicFunctionCache contains a map of topics to functions and filters
type TopicFunctionCache struct {
	topicMap  map[string][]string
	filterMap map[string]string
	lock      sync.RWMutex
}

// NewTopicFunctionCache returns a new instance with both topic and filter maps
func NewTopicFunctionCache() *TopicFunctionCache {
	return &TopicFunctionCache{
		topicMap:  make(map[string][]string),
		filterMap: make(map[string]string),
		lock:      sync.RWMutex{},
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

// GetCachedFilter retrieves the filter associated with a topic, wildcard, or global filters
func (m *TopicFunctionCache) GetCachedFilter(topic string) string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// First, check if a topic-specific filter exists
	if filter, exists := m.filterMap[topic]; exists && filter != "" {
		m.logJSON("info", "Found topic-specific filter", map[string]interface{}{
			"topic":  topic,
			"filter": filter,
		})
		return filter
	}

	// Return an empty string without logging if no valid filters exist
	return ""
}

// Refresh updates the topic cache, ensuring no duplicates are added
func (m *TopicFunctionCache) Refresh(update map[string][]string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Ensure no duplicates are added in the topic map
	for topic, functions := range update {
		existingFunctions, exists := m.topicMap[topic]
		if exists {
			for _, fn := range functions {
				if !contains(existingFunctions, fn) {
					existingFunctions = append(existingFunctions, fn)
				}
			}
			m.topicMap[topic] = existingFunctions
		} else {
			m.topicMap[topic] = functions
		}
	}

	m.logJSON("info", "Topic cache refreshed", map[string]interface{}{
		"cache": m.topicMap,
	})
}

// RefreshFilters updates the filter cache with new values
func (m *TopicFunctionCache) RefreshFilters(update map[string]string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.filterMap = update
	m.logJSON("info", "Filter cache refreshed", map[string]interface{}{
		"cache": m.filterMap,
	})
}

// Helper function to check if a slice contains a specific element
func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
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
