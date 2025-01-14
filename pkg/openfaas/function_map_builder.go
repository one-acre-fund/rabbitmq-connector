/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package openfaas

import "strings"

// TopicMapBuilder defines an interface that allows building a TopicMap
type TopicMapBuilder interface {
	Append(topic string, function string)
	AppendWithFilter(topic, function, filter string)
	Build() (map[string][]string, map[string]map[string]string)
}

// FunctionMapBuilder convenient construct to build a map
type FunctionMapBuilder struct {
	target    map[string][]string
	filterMap map[string]map[string]string // map[functionName]map[topic]filter
}

// NewFunctionMapBuilder returns a new instance with an empty build target
func NewFunctionMapBuilder() *FunctionMapBuilder {
	return &FunctionMapBuilder{
		target:    make(map[string][]string),
		filterMap: make(map[string]map[string]string),
	}
}

// Append the provided function to the specified topic
func (b *FunctionMapBuilder) Append(topic string, function string) {
	key := strings.TrimSpace(topic)

	if len(key) == 0 {
		println("Topic was empty after trimming; will ignore provided function.")
		return
	}

	if b.target[key] == nil {
		b.target[key] = []string{}
	}

	b.target[key] = append(b.target[key], function)
}

// AppendWithFilter appends a topic with its associated function and filter
func (b *FunctionMapBuilder) AppendWithFilter(topic, function, filter string) {
	b.Append(topic, function)

	if b.filterMap[function] == nil {
		b.filterMap[function] = make(map[string]string)
	}

	if filter != "" {
		b.filterMap[function][topic] = filter
	}
}

// Build returns the topic map and function filter map
func (b *FunctionMapBuilder) Build() (map[string][]string, map[string]map[string]string) {
	return b.target, b.filterMap
}
