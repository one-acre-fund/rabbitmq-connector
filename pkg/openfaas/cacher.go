/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package openfaas

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	types2 "github.com/Templum/rabbitmq-connector/pkg/types"

	"github.com/Templum/rabbitmq-connector/pkg/config"
	"github.com/openfaas/faas-provider/types"
)

// Copyright (c) Simon Pelczer 2019. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
	log.Printf("Starting invocation for topic %s", topic)

	var functions []string
	for i := 0; i < 3; i++ { // Retry up to 3 times
		functions = c.cache.GetCachedValues(topic)
		if len(functions) > 0 {
			break
		}
		log.Printf("No functions registered for topic %s, retrying...", topic)
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond) // Exponential backoff
	}

	if len(functions) == 0 {
		log.Printf("No functions registered for topic %s after retries", topic)
		return nil
	}

	for _, fn := range functions {
		startTime := time.Now()
		log.Printf("Invoking function %s for topic %s synchronously", fn, topic)

		var response []byte
		var statusCode int
		var err error
		for i := 0; i < 3; i++ { // Retry up to 3 times for connection errors
			response, statusCode, err = c.client.InvokeSync(context.Background(), fn, invocation)
			if err != nil {
				if strings.Contains(err.Error(), "no free connections available to host") {
					log.Printf("Invocation for function %s failed due to connection error, retrying...", fn)
					time.Sleep(time.Duration(100*(i+1)) * time.Millisecond) // Exponential backoff
					continue
				}
				log.Printf("Invocation for topic %s failed after %.2fs due to err %s", topic, time.Since(startTime).Seconds(), err)
				return err
			}
			break
		}

		if err != nil {
			log.Printf("Invocation for function %s failed after retries due to err %s, switching to async", fn, err)
			for i := 0; i < 3; i++ { // Retry up to 3 times for async connection errors
				_, asyncStatusCode, asyncErr := c.client.InvokeAsync(context.Background(), fn, invocation)
				if asyncErr != nil {
					if strings.Contains(asyncErr.Error(), "no free connections available to host") {
						log.Printf("Async invocation for function %s failed due to connection error, retrying...", fn)
						time.Sleep(time.Duration(100*(i+1)) * time.Millisecond) // Exponential backoff
						continue
					}
					log.Printf("Async invocation for function %s failed due to err %s", fn, asyncErr)
					return asyncErr
				}
				log.Printf("Async invocation for function %s succeeded, status code: %d", fn, asyncStatusCode)
				return nil
			}
			log.Printf("Async invocation for function %s failed after retries due to err %s", fn, err)
			return err
		}

		log.Printf("Invocation for function %s succeeded in %.2fs, status code: %d, response: %s", fn, time.Since(startTime).Seconds(), statusCode, string(response))
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
			log.Println("Received done via context will stop refreshing cache")
			break loop
		}
	}
}

func (c *Controller) refreshTick(ctx context.Context, hasNamespaceSupport bool) {
	builder := NewFunctionMapBuilder()
	var namespaces []string
	var err error

	if hasNamespaceSupport {
		log.Println("Crawling namespaces for functions")
		namespaces, err = c.client.GetNamespaces(ctx)
		if err != nil {
			log.Printf("Received the following error during fetching namespaces %s", err)
			return // Skip updating the cache on error
		}
	} else {
		namespaces = []string{""}
	}

	log.Println("Crawling for functions")
	c.crawlFunctions(ctx, namespaces, builder)

	log.Println("Crawling finished, will now refresh the cache")
	newCache := builder.Build()
	if len(newCache) == 0 {
		log.Printf("New cache is empty, skipping cache refresh")
		return // Skip updating the cache if the new cache is empty
	}
	c.cache.Refresh(newCache)
	log.Printf("Cache refreshed successfully with %d entries", len(newCache))
}

func (c *Controller) crawlFunctions(ctx context.Context, namespaces []string, builder TopicMapBuilder) {
	for _, ns := range namespaces {
		found, err := c.client.GetFunctions(ctx, ns)
		if err != nil {
			log.Printf("Received %s while fetching functions on namespace %s", err, ns)
			found = []types.FunctionStatus{}
		}

		for _, fn := range found {
			topics := c.extractTopicsFromAnnotations(fn)
			log.Printf("Function: %s, Topics: %v", fn.Name, topics)

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
