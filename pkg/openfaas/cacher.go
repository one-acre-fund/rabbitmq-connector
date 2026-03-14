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
	"strconv"
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
// Modify Invoke to handle function-specific filters while keeping existing retry logic
func (c *Controller) Invoke(topic string, invocation *types2.OpenFaaSInvocation) error {
	c.logJSON("info", "Starting invocation for topic", map[string]interface{}{
		"topic":       topic,
		"contentType": invocation.ContentType, // Log the content type
	})

	var functions []string
	for i := 0; i < 3; i++ {
		functions = c.cache.GetCachedValues(topic)
		if len(functions) > 0 {
			break
		}
		c.logJSON("info", "No functions registered for topic, retrying...", map[string]interface{}{
			"topic": topic,
		})
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}

	if len(functions) == 0 {
		c.logJSON("info", "No functions registered for topic after retries", map[string]interface{}{
			"topic": topic,
		})
		return nil
	}

	for _, fn := range functions {
		// Log the start of processing each function
		c.logJSON("info", "Processing function", map[string]interface{}{
			"function": fn,
			"topic":    topic,
		})

		cachedFilter := c.cache.GetCachedFilter(topic, fn)

		if cachedFilter != "" {
			c.logJSON("info", "Applying cached filter for function", map[string]interface{}{
				"function":     fn,
				"cachedFilter": cachedFilter,
			})

			filterResult := c.applyAllFilters(cachedFilter, invocation.Message)

			// Log filter evaluation result
			c.logJSON("info", "Filter evaluation result", map[string]interface{}{
				"function": fn,
				"result":   filterResult,
				"filter":   cachedFilter,
				"message":  string(*invocation.Message),
			})

			if !filterResult {
				c.logJSON("info", "Filters did not match, skipping function", map[string]interface{}{
					"function": fn,
				})
				continue
			}
		}

		// Ensure content type is set if not already
		if invocation.ContentType == "" {
			invocation.ContentType = "application/json"
			c.logJSON("info", "Set default content type", map[string]interface{}{
				"function":    fn,
				"contentType": invocation.ContentType,
			})
		}

		// Log attempt details
		c.logJSON("info", "Starting function invocation", map[string]interface{}{
			"function":    fn,
			"topic":       topic,
			"contentType": invocation.ContentType,
			"message":     string(*invocation.Message),
		})

		startTime := time.Now()
		var response []byte
		var statusCode int
		var err error

		// Sync invocation with retries
		for i := 0; i < 3; i++ {
			c.logJSON("info", "Attempting sync invocation", map[string]interface{}{
				"function":    fn,
				"attempt":     i + 1,
				"contentType": invocation.ContentType,
			})

			response, statusCode, err = c.client.InvokeSync(context.Background(), fn, invocation)
			if err != nil {
				c.logJSON("error", "Invocation failed, retrying...", map[string]interface{}{
					"function": fn,
					"error":    err,
					"attempt":  i + 1,
				})
				time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
				continue
			}
			break
		}

		if err != nil {
			c.logJSON("error", "Invocation failed after retries, switching to async", map[string]interface{}{
				"function": fn,
				"error":    err,
			})

			// Async fallback with retries
			for i := 0; i < 3; i++ {
				c.logJSON("info", "Attempting async invocation", map[string]interface{}{
					"function": fn,
					"attempt":  i + 1,
				})

				_, asyncStatusCode, asyncErr := c.client.InvokeAsync(context.Background(), fn, invocation)
				if asyncErr != nil {
					c.logJSON("error", "Async invocation failed, retrying...", map[string]interface{}{
						"function": fn,
						"error":    asyncErr,
						"attempt":  i + 1,
					})
					time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
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
			"function":    fn,
			"duration":    time.Since(startTime).Seconds(),
			"status":      statusCode,
			"response":    string(response),
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
			"contentType": invocation.ContentType,
		})
	}
	return nil
}
func (c *Controller) applyAllFilters(cachedFilter string, message *[]byte) bool {
	var payload map[string]interface{}

	// Unmarshal the message into a map
	if err := json.Unmarshal(*message, &payload); err != nil {
		c.logJSON("error", "Failed to unmarshal payload for filter evaluation", map[string]interface{}{
			"error": err,
		})
		return false
	}

	// If there's no filter defined, return true and allow invocation to proceed
	if cachedFilter == "" {
		c.logJSON("info", "No filter applied, proceeding with function invocation", nil)
		return true
	}

	// Decode encoded operators BEFORE splitting by && / ||
	cachedFilter = decodeAnnotations(cachedFilter)

	// Helper function to split AND/OR conditions and return whether they pass or fail
	processAndConditions := func(filter string) bool {
		andParts := strings.Split(filter, " && ")

		for _, andPart := range andParts {
			condition := strings.TrimSpace(andPart)

			// Evaluate the condition
			evalResult, valid := evaluateCondition(condition, payload)
			if !valid {
				c.logJSON("warning", "Could not parse condition, skipping", map[string]interface{}{
					"condition": condition,
				})
				return false
			}
			if !evalResult {
				c.logJSON("info", "Condition did not match", map[string]interface{}{
					"condition": condition,
				})
				return false
			}
		}

		return true
	}

	// Process OR conditions; any OR condition that passes means the filter passes
	orParts := strings.Split(cachedFilter, " || ")
	for _, orPart := range orParts {
		orPart = strings.TrimSpace(orPart)
		if processAndConditions(orPart) {
			// If any OR part passes, the filter passes
			return true
		}
	}

	// If no OR condition passed, the filter does not match
	return false
}

func decodeAnnotations(input string) string {
	replacements := []struct {
		old string
		new string
	}{
		{`\u0026\u0026`, `&&`},   // Unicode for "&&"
		{`\u007C\u007C`, `||`},   // Unicode for "||"
		{`\\\"`, `"`},            // Escaped quotes
		{`\\\\`, `\`},            // Escaped backslashes
		{`\"`, `"`},              // Replace any single escaped quote
	}

	for _, r := range replacements {
		input = strings.ReplaceAll(input, r.old, r.new)
	}

	return input
}


// mapLookup tries an exact key match, then falls back to case-insensitive.
func mapLookup(m map[string]interface{}, key string) (interface{}, bool) {
	if val, exists := m[key]; exists {
		return val, true
	}
	lowerKey := strings.ToLower(key)
	for k, v := range m {
		if strings.ToLower(k) == lowerKey {
			return v, true
		}
	}
	return nil, false
}

func getNestedValue(key string, data map[string]interface{}) (interface{}, bool) {
	parts := strings.Split(key, ".")
	var current interface{} = data

	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		current, ok = mapLookup(m, part)
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// compareString returns a condition evaluator for string comparison operators (==, !=).
// defaultOnMissing controls the result when the key is not found in the payload.
func compareString(fn func(a, e string) bool, defaultOnMissing bool) func(string, string, map[string]interface{}) (bool, bool) {
	return func(key, expected string, payload map[string]interface{}) (bool, bool) {
		actual, ok := getNestedValue(key, payload)
		if !ok {
			return defaultOnMissing, true
		}
		return fn(fmt.Sprintf("%v", actual), expected), true
	}
}

// compareNumeric returns a condition evaluator for numeric comparison operators (>, <, >=, <=).
func compareNumeric(fn func(a, e float64) bool) func(string, string, map[string]interface{}) (bool, bool) {
	return func(key, expected string, payload map[string]interface{}) (bool, bool) {
		expectedFloat, err := strconv.ParseFloat(expected, 64)
		if err != nil {
			return false, false
		}
		actual, ok := getNestedValue(key, payload)
		if !ok {
			return false, true
		}
		actualFloat, err := strconv.ParseFloat(fmt.Sprintf("%v", actual), 64)
		if err != nil {
			return false, false
		}
		return fn(actualFloat, expectedFloat), true
	}
}

// evaluateContains handles the key.Contains("value") syntax.
func evaluateContains(condition string, payload map[string]interface{}) (bool, bool) {
	containsIndex := strings.Index(condition, ".Contains(")
	if containsIndex == -1 {
		return false, false
	}
	key := strings.TrimSpace(condition[:containsIndex])
	containsValue := strings.Trim(strings.TrimSuffix(strings.TrimSpace(condition[containsIndex+10:]), ")"), `"`)

	actualValue, ok := getNestedValue(key, payload)
	if !ok {
		return false, true
	}
	return strings.Contains(
		strings.ToLower(fmt.Sprintf("%v", actualValue)),
		strings.ToLower(containsValue),
	), true
}

// Operator table: multi-char operators must come before single-char to avoid false matches.
var operatorTable = []struct {
	op      string
	compare func(key, expected string, payload map[string]interface{}) (bool, bool)
}{
	{"!=", compareString(func(a, e string) bool { return a != e }, true)},
	{">=", compareNumeric(func(a, e float64) bool { return a >= e })},
	{"<=", compareNumeric(func(a, e float64) bool { return a <= e })},
	{"==", compareString(func(a, e string) bool { return a == e }, false)},
	{">", compareNumeric(func(a, e float64) bool { return a > e })},
	{"<", compareNumeric(func(a, e float64) bool { return a < e })},
}

// evaluateCondition returns (result, keyExists) where result is whether the condition
// passed and keyExists is whether the referenced key was found in the payload.
func evaluateCondition(condition string, payload map[string]interface{}) (bool, bool) {
	// Handle Contains check first (different syntax: key.Contains("value"))
	if strings.Contains(condition, ".Contains(") {
		return evaluateContains(condition, payload)
	}

	// Find the first matching operator and evaluate
	for _, op := range operatorTable {
		if idx := strings.Index(condition, op.op); idx != -1 {
			key := strings.TrimSpace(condition[:idx])
			value := strings.TrimSpace(condition[idx+len(op.op):])
			value = strings.Trim(value, `"`)
			return op.compare(key, value, payload)
		}
	}

	return false, false
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
			namespaces = []string{}
		}
	} else {
		namespaces = []string{""}
	}

	c.logJSON("info", "Crawling for functions", nil)
	c.crawlFunctions(ctx, namespaces, builder)

	c.logJSON("info", "Crawling finished, refreshing cache", nil)

	// Retrieve both topic map and filter map from the builder
	topicMap, filterMap := builder.Build()

	c.cache.Refresh(topicMap)         // Refresh the topic cache
	c.cache.RefreshFilters(filterMap) // Refresh the filter cache

	c.logJSON("info", "Cache refreshed successfully", map[string]interface{}{
		"entries": len(topicMap),
	})
}

// Only modify the minimum required parts of crawlFunctions while preserving logging and other logic
func (c *Controller) crawlFunctions(ctx context.Context, namespaces []string, builder TopicMapBuilder) {
	for _, ns := range namespaces {
		found, err := c.client.GetFunctions(ctx, ns)
		if err != nil {
			c.logJSON("error", "Error fetching functions in namespace", map[string]interface{}{
				"namespace": ns,
				"error":     err,
			})
			continue
		}

		for _, fn := range found {
			topics := c.extractTopicsFromAnnotations(fn)
			filters := c.extractFiltersFromAnnotations(fn)

			c.logJSON("info", "Function details", map[string]interface{}{
				"function": fn.Name,
				"topics":   topics,
				"filters":  filters,
			})

			for _, topic := range topics {
				// Store filters specific to this function
				finalFilter := ""

				// Step 1: Append specific filter if it exists
				if filter, ok := filters[topic]; ok {
					finalFilter = appendIfNotContains(finalFilter, filter)
				}

				// Step 2: Append wildcard filter (e.g., "sanction.screen.all")
				topicParts := strings.Split(topic, ".")
				if len(topicParts) >= 2 {
					wildcardFilterKey := strings.Join(topicParts[:2], ".") + ".all"
					if wildcardFilter, ok := filters[wildcardFilterKey]; ok {
						finalFilter = appendIfNotContains(finalFilter, wildcardFilter)
					}
				}

				// Step 3: Append global filter (e.g., "all")
				if globalFilter, ok := filters["all"]; ok {
					finalFilter = appendIfNotContains(finalFilter, globalFilter)
				}

				c.logJSON("info", "Combined filters for topic", map[string]interface{}{
					"filters":  finalFilter,
					"topic":    topic,
					"function": fn.Name, // Add function name to logging
				})

				// Cache the final combined filter with function name
				builder.AppendWithFilter(strings.TrimSpace(topic), fn.Name, finalFilter)
			}
		}
	}
}

// Helper function to append a filter if it doesn't already exist in the final string
func appendIfNotContains(finalFilter, newFilter string) string {
	// Split finalFilter and newFilter into OR conditions
	finalOrConditions := strings.Split(finalFilter, " || ")
	newOrConditions := strings.Split(newFilter, " || ")

	// Use a map to deduplicate conditions
	conditionSet := make(map[string]struct{})
	for _, condition := range finalOrConditions {
		trimmed := strings.TrimSpace(condition)
		if trimmed != "" {
			conditionSet[trimmed] = struct{}{}
		}
	}

	for _, condition := range newOrConditions {
		trimmed := strings.TrimSpace(condition)
		if trimmed != "" {
			conditionSet[trimmed] = struct{}{}
		}
	}

	// Combine unique conditions back with " || "
	var result []string
	for condition := range conditionSet {
		result = append(result, condition)
	}

	return strings.Join(result, " || ")
}

// Extract filters from function annotations
func (c *Controller) extractFiltersFromAnnotations(fn types.FunctionStatus) map[string]string {
	filters := make(map[string]string)

	if fn.Annotations != nil {
		annotations := *fn.Annotations
		c.logJSON("info", "Annotations found", map[string]interface{}{
			"annotations": annotations,
		})

		for key, value := range annotations {
			if strings.HasPrefix(key, "filter-") {
				topic := strings.TrimPrefix(key, "filter-")
				topic = strings.TrimSpace(topic) // Ensure no leading or trailing spaces
				filters[topic] = value
			}
		}
	}
	c.logJSON("info", "Extracted filters from annotations", map[string]interface{}{
		"filters": filters,
	})
	return filters
}

func (c *Controller) extractTopicsFromAnnotations(fn types.FunctionStatus) []string {
	topics := []string{}

	if fn.Annotations != nil {
		annotations := *fn.Annotations
		topicNames := ""
		if val, exist := annotations["topics"]; exist {
			topicNames = val
		} else if val, exist := annotations["topic"]; exist {
			topicNames = val
		}
		if topicNames != "" {
			rawTopics := strings.Split(topicNames, ",")
			for _, topic := range rawTopics {
				topics = append(topics, strings.TrimSpace(topic))
			}
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
