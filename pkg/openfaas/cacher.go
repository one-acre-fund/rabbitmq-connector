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

	"github.com/Knetic/govaluate"
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

	// Helper function to split AND/OR conditions and return whether they pass or fail
	processAndConditions := func(filter string) bool {
		andParts := strings.Split(filter, " && ")
		atLeastOneConditionPassed := false
		for _, andPart := range andParts {
			condition := strings.TrimSpace(andPart)

			// Evaluate each condition; ignore missing keys but track success
			evalResult, keyExists := evaluateCondition(condition, payload)
			if !keyExists {
				// Log a warning if the key doesn't exist
				c.logJSON("warning", "Key not found in payload, skipping condition", map[string]interface{}{
					"condition": condition,
				})
				continue
			}
			if keyExists && !evalResult {
				// If a known condition fails, the whole AND condition fails
				c.logJSON("info", "Condition did not match", map[string]interface{}{
					"condition": condition,
				})
				return false
			}
			// If any condition passes, track that at least one succeeded
			if evalResult {
				atLeastOneConditionPassed = true
			}
		}
		// Return true if at least one condition passed
		return atLeastOneConditionPassed
	}

	// Process OR conditions; any OR condition that passes means the filter passes
	orParts := strings.Split(cachedFilter, " || ")
	for _, orPart := range orParts {
		if processAndConditions(orPart) {
			// If any OR part passes, the filter passes
			return true
		}
	}

	// If no OR condition passed, the filter does not match
	return false
}

func getNestedValue(key string, data map[string]interface{}) (interface{}, bool) {
	parts := strings.Split(key, ".")
	var current interface{} = data

	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			// Try exact match first
			if val, exists := m[part]; exists {
				current = val
				continue
			}

			// Case-insensitive lookup
			lowerPart := strings.ToLower(part)
			for k, v := range m {
				if strings.ToLower(k) == lowerPart {
					current = v
					break
				}
			}
		} else {
			return nil, false
		}
	}
	return current, true
}

// evaluateCondition returns (bool, bool) where the first bool is whether the condition passed and the second is whether the key existed in the payload.
func evaluateCondition(condition string, payload map[string]interface{}) (bool, bool) {

	// Handle Contains check first (e.g., id.Contains("Gonzalo"))
	if strings.Contains(condition, "Contains(") {
		containsIndex := strings.Index(condition, "Contains(")
		if containsIndex == -1 {
			return false, false
		}

		key := strings.TrimSpace(condition[:containsIndex])
		key = strings.TrimSuffix(key, ".")
		containsValue := strings.Trim(strings.TrimSuffix(strings.TrimSpace(condition[containsIndex+9:]), ")"), `"`)

		actualValue, ok := getNestedValue(key, payload)
		if !ok {
			return false, false
		}

		// Case-insensitive contains check
		return strings.Contains(
			strings.ToLower(fmt.Sprintf("%v", actualValue)),
			strings.ToLower(containsValue),
		), true
	}

	// Handle equality checks (e.g., ref == "boy")
	if strings.Contains(condition, "==") {
		parts := strings.Split(condition, "==")
		if len(parts) != 2 {
			return false, false
		}

		key := strings.TrimSpace(parts[0])
		expectedValue := strings.Trim(strings.TrimSpace(parts[1]), `"`)

		// Get the actual value from the payload (handle nested keys)
		actualValue, ok := getNestedValue(key, payload)
		if !ok {
			// Key not found in payload, ignore this condition
			return false, false
		}

		// Compare actual value and expected value
		return fmt.Sprintf("%v", actualValue) == expectedValue, true
	}

	// Handle greater than or equal to (e.g., quantity >= 10)
	if strings.Contains(condition, ">=") {
		parts := strings.Split(condition, ">=")
		if len(parts) != 2 {
			return false, false
		}

		key := strings.TrimSpace(parts[0])
		expectedValue, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return false, false
		}

		// Get the actual value from the payload (handle nested keys)
		actualValue, ok := getNestedValue(key, payload)
		if !ok {
			// Key not found in payload, ignore this condition
			return false, false
		}

		// Convert actual value to a float for comparison
		actualFloat, err := strconv.ParseFloat(fmt.Sprintf("%v", actualValue), 64)
		if err != nil {
			return false, false
		}

		// Perform comparison
		return actualFloat >= expectedValue, true
	}

	// Handle less than or equal to (e.g., quantity <= 20)
	if strings.Contains(condition, "<=") {
		parts := strings.Split(condition, "<=")
		if len(parts) != 2 {
			return false, false
		}

		key := strings.TrimSpace(parts[0])
		expectedValue, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return false, false
		}

		// Get the actual value from the payload (handle nested keys)
		actualValue, ok := getNestedValue(key, payload)
		if !ok {
			// Key not found in payload, ignore this condition
			return false, false
		}

		// Convert actual value to a float for comparison
		actualFloat, err := strconv.ParseFloat(fmt.Sprintf("%v", actualValue), 64)
		if err != nil {
			return false, false
		}

		// Perform comparison
		return actualFloat <= expectedValue, true
	}

	// Handle greater than (e.g., quantity > 10)
	if strings.Contains(condition, ">") {
		parts := strings.Split(condition, ">")
		if len(parts) != 2 {
			return false, false
		}

		key := strings.TrimSpace(parts[0])
		expectedValue, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return false, false
		}

		// Get the actual value from the payload (handle nested keys)
		actualValue, ok := getNestedValue(key, payload)
		if !ok {
			// Key not found in payload, ignore this condition
			return false, false
		}

		// Convert actual value to a float for comparison
		actualFloat, err := strconv.ParseFloat(fmt.Sprintf("%v", actualValue), 64)
		if err != nil {
			return false, false
		}

		// Perform comparison
		return actualFloat > expectedValue, true
	}

	// Handle less than (e.g., quantity < 5)
	if strings.Contains(condition, "<") {
		parts := strings.Split(condition, "<")
		if len(parts) != 2 {
			return false, false
		}

		key := strings.TrimSpace(parts[0])
		expectedValue, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return false, false
		}

		// Get the actual value from the payload (handle nested keys)
		actualValue, ok := getNestedValue(key, payload)
		if !ok {
			// Key not found in payload, ignore this condition
			return false, false
		}

		// Convert actual value to a float for comparison
		actualFloat, err := strconv.ParseFloat(fmt.Sprintf("%v", actualValue), 64)
		if err != nil {
			return false, false
		}

		// Perform comparison
		return actualFloat < expectedValue, true
	}

	// Return false for unhandled conditions
	return false, false
}

func (c *Controller) applyFilter(filter string, message *[]byte) bool {
	var payload map[string]interface{}

	// Unmarshal the payload into a map
	if err := json.Unmarshal(*message, &payload); err != nil {
		c.logJSON("error", "Failed to unmarshal payload for filter evaluation", map[string]interface{}{
			"error": err,
		})
		return false
	}

	// Define a custom 'Contains' function for string matching
	functions := map[string]govaluate.ExpressionFunction{
		"Contains": func(args ...interface{}) (interface{}, error) {
			if len(args) != 2 {
				return false, fmt.Errorf("Contains expects exactly two arguments")
			}

			str, ok1 := args[0].(string)
			substr, ok2 := args[1].(string)
			if !ok1 || !ok2 {
				return false, fmt.Errorf("Contains arguments must be strings")
			}

			return strings.Contains(str, substr), nil
		},
	}

	// Parse the filter expression with custom functions
	expression, err := govaluate.NewEvaluableExpressionWithFunctions(filter, functions)
	if err != nil {
		c.logJSON("error", "Failed to parse filter expression", map[string]interface{}{
			"error": err,
		})
		return false
	}

	// Evaluate the expression with the payload as parameters
	result, err := expression.Evaluate(payload)
	if err != nil {
		c.logJSON("error", "Error evaluating filter expression", map[string]interface{}{
			"error":  err,
			"filter": filter,
		})
		return false
	}

	// Ensure the result is boolean
	boolResult, ok := result.(bool)
	if !ok {
		c.logJSON("error", "Filter did not return a boolean result", map[string]interface{}{
			"filter": filter,
		})
		return false
	}

	return boolResult
}

// evaluateFilter evaluates the filter expression against the given payload.
func evaluateFilter(filter string, payload map[string]interface{}) (bool, error) {
	// Parse the filter expression
	expression, err := govaluate.NewEvaluableExpression(filter)
	if err != nil {
		return false, fmt.Errorf("failed to parse filter expression: %v", err)
	}

	// Evaluate the expression against the payload (using payload as parameters)
	result, err := expression.Evaluate(payload)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate filter: %v", err)
	}

	// Ensure the result is a boolean (true/false)
	boolResult, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("filter did not return a boolean result")
	}

	return boolResult, nil
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

	// Retrieve both topic map and filter map from the builder
	topicMap, filterMap := builder.Build()

	if len(topicMap) == 0 {
		c.logJSON("info", "New cache is empty, skipping cache refresh", nil)
		return // Skip updating the cache if the new cache is empty
	}

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
				wildcardFilterKey := strings.Join(strings.Split(topic, ".")[:2], ".") + ".all"
				if wildcardFilter, ok := filters[wildcardFilterKey]; ok {
					finalFilter = appendIfNotContains(finalFilter, wildcardFilter)
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
// Helper function to append a filter if it doesn't already exist in the final string
func appendIfNotContains(finalFilter, newFilter string) string {
	// Split finalFilter and newFilter into OR conditions
	finalOrConditions := strings.Split(finalFilter, " || ")
	newOrConditions := strings.Split(newFilter, " || ")

	// Process each OR condition
	for _, newOrCondition := range newOrConditions {
		newAndConditions := strings.Split(newOrCondition, " && ")
		for _, newAndCondition := range newAndConditions {
			newAndCondition = strings.TrimSpace(newAndCondition)

			// Check for duplicates in OR and AND conditions
			if !conditionExists(finalOrConditions, newAndCondition) {
				finalOrConditions = append(finalOrConditions, newAndCondition)
			}
		}
	}

	// Join OR conditions back into a single string, removing duplicates
	finalFilter = strings.Join(finalOrConditions, " || ")

	// Remove leading/trailing '&&' or '||' and other possible trailing logical symbols
	finalFilter = strings.TrimPrefix(finalFilter, " && ")
	finalFilter = strings.TrimPrefix(finalFilter, " || ")
	finalFilter = strings.TrimSuffix(finalFilter, " && ")
	finalFilter = strings.TrimSuffix(finalFilter, " || ")
	finalFilter = strings.TrimSuffix(finalFilter, " >= ")
	finalFilter = strings.TrimSuffix(finalFilter, " <= ")
	finalFilter = strings.TrimSuffix(finalFilter, " > ")
	finalFilter = strings.TrimSuffix(finalFilter, " < ")

	return finalFilter
}

// Helper function to check if a condition already exists in the list
func conditionExists(conditions []string, condition string) bool {
	for _, cond := range conditions {
		if strings.TrimSpace(cond) == condition {
			return true
		}
	}
	return false
}

// Helper function to extract keys from a map (used for combining unique filters)
func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func (c *Controller) getWildcardFilterForTopic(topic string) string {
	// Modify the topic to append ".all" by removing the last part of the topic
	topicParts := strings.Split(topic, ".")
	if len(topicParts) > 1 {
		wildcardTopic := strings.Join(topicParts[:len(topicParts)-1], ".") + ".all"
		c.logJSON("info", "Generated wildcard topic", map[string]interface{}{
			"wildcardTopic": wildcardTopic,
		})
		return wildcardTopic // Directly return the modified wildcard topic
	}

	// If no wildcard filter can be created, return an empty string
	return ""
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
		if topicNames, exist := annotations["topics"]; exist {
			// Split and trim each topic name to ensure no leading/trailing whitespace
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
