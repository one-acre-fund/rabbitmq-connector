/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package openfaas

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	types2 "github.com/Templum/rabbitmq-connector/pkg/types"

	"github.com/Templum/rabbitmq-connector/pkg/config"
	"github.com/openfaas/faas-provider/types"
)

// Controller is responsible for building up and maintaining a
// Cache with all of the deployed OpenFaaS Functions across
// all namespaces
type Controller struct {
	conf   *config.Controller
	client FunctionCrawler
	cache  TopicMap
}

// NewController returns a new instance
func NewController(conf *config.Controller, client FunctionCrawler, cache TopicMap) *Controller {
	return &Controller{
		conf:   conf,
		client: client,
		cache:  cache,
	}
}

// Start setups the cache and starts continuous caching
func (c *Controller) Start(ctx context.Context) {
	hasNamespaceSupport, _ := c.client.HasNamespaceSupport(ctx)
	timer := time.NewTicker(c.conf.TopicRefreshTime)

	// Initial populating
	c.refreshTick(ctx, hasNamespaceSupport)
	go c.refresh(ctx, timer, hasNamespaceSupport)
}

// Invoke triggers a call to all functions registered to the specified topic. It will abort invocation in case it encounters an error
func (c *Controller) Invoke(topic string, invocation *types2.OpenFaaSInvocation) error {
	c.logJSON("info", "Starting invocation for topic", map[string]interface{}{
		"topic": topic,
	})

	var functions []string
	for i := 0; i < 3; i++ { // Retry up to 3 times
		functions = c.cache.GetCachedValues(topic)
		if len(functions) > 0 {
			break
		}
		c.logJSON("info", "No functions registered for topic, retrying...", map[string]interface{}{
			"topic": topic,
		})
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond) // Exponential backoff
	}

	if len(functions) == 0 {
		c.logJSON("info", "No functions registered for topic after retries", map[string]interface{}{
			"topic": topic,
		})
		return nil
	}

	for _, fn := range functions {
		startTime := time.Now()
		c.logJSON("info", "Invoking function synchronously", map[string]interface{}{
			"function": fn,
			"topic":    topic,
		})

		var response []byte
		var statusCode int
		var err error
		for i := 0; i < 3; i++ { // Retry up to 3 times for any errors
			response, statusCode, err = c.client.InvokeSync(context.Background(), fn, invocation)
			if err != nil {
				c.logJSON("error", "Invocation failed, retrying...", map[string]interface{}{
					"function": fn,
					"error":    err,
				})
				time.Sleep(time.Duration(100*(i+1)) * time.Millisecond) // Exponential backoff
				continue
			}
			break
		}

		if err != nil {
			c.logJSON("error", "Invocation failed after retries, switching to async", map[string]interface{}{
				"function": fn,
				"error":    err,
			})
			for i := 0; i < 3; i++ { // Retry up to 3 times for async errors
				_, asyncStatusCode, asyncErr := c.client.InvokeAsync(context.Background(), fn, invocation)
				if asyncErr != nil {
					c.logJSON("error", "Async invocation failed, retrying...", map[string]interface{}{
						"function": fn,
						"error":    asyncErr,
					})
					time.Sleep(time.Duration(100*(i+1)) * time.Millisecond) // Exponential backoff
					continue
				}
				c.logJSON("info", "Async invocation succeeded", map[string]interface{}{
					"function": fn,
					"status":   asyncStatusCode,
				})
				return nil
			}
			c.logJSON("error", "Async invocation failed after retries", map[string]interface{}{
				"function": fn,
				"error":    err,
			})
			return err
		}

		c.logJSON("info", "Invocation succeeded", map[string]interface{}{
			"function":  fn,
			"duration":  time.Since(startTime).Seconds(),
			"status":    statusCode,
			"response":  string(response),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
	return nil
}

func (c *Controller) refresh(ctx context.Context, ticker *time.Ticker, hasNamespaceSupport bool) {
loop:
	for {
		select {
		case <-ticker.C:
			c.refreshTick(ctx, hasNamespaceSupport)
			break
		case <-ctx.Done():
			c.logJSON("info", "Received done via context will stop refreshing cache", nil)
			break loop
		}
	}
}

func (c *Controller) refreshTick(ctx context.Context, hasNamespaceSupport bool) {
	builder := NewFunctionMapBuilder()
	var namespaces []string
	var err error

	if hasNamespaceSupport {
		c.logJSON("info", "Crawling namespaces for functions", nil)
		namespaces, err = c.client.GetNamespaces(ctx)
		if err != nil {
			c.logJSON("error", "Error fetching namespaces", map[string]interface{}{
				"error": err,
			})
			return // Skip updating the cache on error
		}
	} else {
		namespaces = []string{""}
	}

	c.logJSON("info", "Crawling for functions", nil)
	c.crawlFunctions(ctx, namespaces, builder)

	c.logJSON("info", "Crawling finished, refreshing cache", nil)
	newCache := builder.Build()
	if len(newCache) == 0 {
		c.logJSON("info", "New cache is empty, skipping cache refresh", nil)
		return // Skip updating the cache if the new cache is empty
	}
	c.cache.Refresh(newCache)
	c.logJSON("info", "Cache refreshed successfully", map[string]interface{}{
		"entries": len(newCache),
	})
}

func (c *Controller) crawlFunctions(ctx context.Context, namespaces []string, builder TopicMapBuilder) {
	for _, ns := range namespaces {
		found, err := c.client.GetFunctions(ctx, ns)
		if err != nil {
			c.logJSON("error", "Error fetching functions in namespace", map[string]interface{}{
				"namespace": ns,
				"error":     err,
			})
			found = []types.FunctionStatus{}
		}

		for _, fn := range found {
			topics := c.extractTopicsFromAnnotations(fn)
			c.logJSON("info", "Found function with topics", map[string]interface{}{
				"function": fn.Name,
				"topics":   topics,
			})

			for _, topic := range topics {
				if len(ns) > 0 {
					builder.Append(topic, fmt.Sprintf("%s.%s", fn.Name, ns)) // Include Namespace to call the correct function
				} else {
					builder.Append(topic, fn.Name)
				}
			}
		}
	}
}

func (c *Controller) extractTopicsFromAnnotations(fn types.FunctionStatus) []string {
	topics := []string{}

	if fn.Annotations != nil {
		annotations := *fn.Annotations
		if topicNames, exist := annotations["topics"]; exist {
			topics = strings.Split(topicNames, ",")
		}
	}

	return topics
}

func (c *Controller) logJSON(level, message string, fields map[string]interface{}) {
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
