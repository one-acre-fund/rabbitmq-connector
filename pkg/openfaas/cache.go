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
	GetCachedFilter(topic string, functionName string) string
	Refresh(topicMap map[string][]string)
	RefreshFilters(filterMap map[string]map[string]string)
}

// TopicFunctionCache contains a map of topics to functions and filters
type TopicFunctionCache struct {
	topicMap       map[string][]string
	functionFilter map[string]map[string]string // map[functionName]map[topic]filter
	lock           sync.RWMutex
}

// NewTopicFunctionCache returns a new instance with both topic and filter maps
func NewTopicFunctionCache() *TopicFunctionCache {
	return &TopicFunctionCache{
		topicMap:       make(map[string][]string),
		functionFilter: make(map[string]map[string]string),
		lock:           sync.RWMutex{},
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
func (m *TopicFunctionCache) GetCachedFilter(topic string, functionName string) string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// Check if we have function-specific filters
	if functionFilters, exists := m.functionFilter[functionName]; exists {
		// Check for topic-specific filter
		if filter, exists := functionFilters[topic]; exists && filter != "" {
			m.logJSON("info", "Found topic-specific filter for function", map[string]interface{}{
				"topic":    topic,
				"function": functionName,
				"filter":   filter,
			})
			return filter
		}
		// Check for global filter for this function
		if filter, exists := functionFilters["all"]; exists && filter != "" {
			m.logJSON("info", "Using global filter for function", map[string]interface{}{
				"function": functionName,
				"filter":   filter,
			})
			return filter
		}
	}

	return ""
}

func (m *TopicFunctionCache) Refresh(update map[string][]string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.topicMap = update

	// Debug logging for topic mappings
	for topic, functions := range update {
		m.logJSON("debug", "Cached topic mapping", map[string]interface{}{
			"topic":     topic,
			"functions": functions,
		})
	}
}

func (m *TopicFunctionCache) RefreshFilters(update map[string]map[string]string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.functionFilter = update

	// Debug logging for cache contents
	for fn, filters := range update {
		m.logJSON("debug", "Cached filters for function", map[string]interface{}{
			"function": fn,
			"filters":  filters,
		})
	}
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
