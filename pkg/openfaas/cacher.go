/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package openfaas

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Knetic/govaluate"
	types2 "github.com/Templum/rabbitmq-connector/pkg/types"
	"log"
	"strconv"
	"strings"
	"time"

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

	// Get the cached filter for the topic, which already includes topic-specific, wildcard, and global filters
	cachedFilter := c.cache.GetCachedFilter(topic)

	for _, fn := range functions {

		// Only log if there's an actual filter to apply
		if cachedFilter != "" {
			c.logJSON("info", "Applying cached filter for function", map[string]interface{}{
				"function":     fn,
				"cachedFilter": cachedFilter,
			})

			// Apply the cached filter
			if !c.applyAllFilters(cachedFilter, invocation.Message) {
				c.logJSON("info", "Filters did not match, skipping function", map[string]interface{}{
					"function": fn,
				})
				continue
			}
		}

		startTime := time.Now()
		// Proceed with function invocation if the filter passed
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

// evaluateCondition returns (bool, bool) where the first bool is whether the condition passed and the second is whether the key existed in the payload.
func evaluateCondition(condition string, payload map[string]interface{}) (bool, bool) {
	// Handle Contains check first (e.g., id.Contains("Gonzalo"))
	if strings.Contains(condition, "Contains(") {
		containsIndex := strings.Index(condition, "Contains(")
		if containsIndex == -1 {
			return false, false
		}

		// Extract key and value from Contains condition
		key := strings.TrimSpace(condition[:containsIndex]) // Extract key before 'Contains'
		key = strings.TrimSuffix(key, ".")                  // Ensure no trailing dot
		containsValue := strings.Trim(strings.TrimSuffix(strings.TrimSpace(condition[containsIndex+9:]), ")"), `"`)

		// Get the actual value from the payload
		actualValue, ok := payload[key]
		if !ok {
			// Key not found in payload, ignore this condition
			return false, false
		}

		// Check if the actual value contains the expected substring
		return strings.Contains(fmt.Sprintf("%v", actualValue), containsValue), true
	}

	// Handle equality checks (e.g., ref == "boy")
	if strings.Contains(condition, "==") {
		parts := strings.Split(condition, "==")
		if len(parts) != 2 {
			return false, false
		}

		key := strings.TrimSpace(parts[0])
		expectedValue := strings.Trim(strings.TrimSpace(parts[1]), `"`)

		// Get the actual value from the payload
		actualValue, ok := payload[key]
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

		// Get the actual value from the payload
		actualValue, ok := payload[key]
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

		// Get the actual value from the payload
		actualValue, ok := payload[key]
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

		// Get the actual value from the payload
		actualValue, ok := payload[key]
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

		// Get the actual value from the payload
		actualValue, ok := payload[key]
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

func (c *Controller) getFilterForTopic(topic string) string {
	// Check for a topic-specific filter first
	filter := c.cache.GetCachedFilter(topic)

	// If no topic-specific filter is found, check for a wildcard filter (e.g., "sanction.screen.all")
	if filter == "" {
		// Create a wildcard pattern by taking the first two segments of the topic
		topicParts := strings.Split(topic, ".")
		if len(topicParts) > 1 {
			topicPrefix := strings.Join(topicParts[:2], ".")
			filter = c.cache.GetCachedFilter("filter-" + topicPrefix + ".all")
		}
	}

	// If still no filter is found, check for a global filter ("filter-all")
	if filter == "" {
		filter = c.cache.GetCachedFilter("filter-all")
	}

	return filter
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
				// String to store combined filters
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
					"filters": finalFilter,
					"topic":   topic,
				})

				// Cache the final combined filter
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
