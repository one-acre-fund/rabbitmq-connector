/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package types

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

// Topology definition
type Topology []struct {
	Name        string            `json:"name"`
	Topics      []string          `json:"topics"`
	Queue       string            `json:"queue"`
	Declare     bool              `json:"declare"`
	Type        string            `json:"type,omitempty"`
	Durable     bool              `json:"durable,omitempty"`
	AutoDeleted bool              `json:"auto-deleted,omitempty"`
	TTL         int               `json:"ttl,omitempty"`
	DLE         string            `json:"dle,omitempty"`
	Bypass      bool              `json:"bypass,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
}

// Exchange Definition of a RabbitMQ Exchange
type Exchange struct {
	Name        string
	Topics      []string
	Queue       string
	Declare     bool
	Type        string
	Durable     bool
	AutoDeleted bool
	TTL         int
	DLE         string
	Bypass      bool `json:"bypass,omitempty"`
	Filters     map[string]string
}

// EnsureCorrectType is responsible to make sure that the read-in type is one of the allowed
// which right now is direct or topic. If it is not a valid type, will default to direct.
func (e *Exchange) EnsureCorrectType() {
	switch strings.ToLower(e.Type) {
	case "direct":
		e.Type = "direct"
	case "topic":
		e.Type = "topic"
	default:
		e.Type = "direct"
	}
}

// ReadTopologyFromFile reads a topology file in yaml format from the specified path.
// Further it parses the file and returns it already in the Topology struct format.
func ReadTopologyFromFile(fs afero.Fs, path string) (Topology, error) {
	yamlFile, err := afero.ReadFile(fs, path)
	if err != nil {
		return Topology{}, err
	}

	var out Topology
	err = yaml.Unmarshal(yamlFile, &out)
	if err != nil {
		return Topology{}, err
	}
	// Log the topology for debugging
	for _, entry := range out {
		logJSON("info", "ReadTopologyFromFile", map[string]interface{}{
			"Exchange": entry.Name,
			"Topics":   entry.Topics,
			"Filters":  entry.Filters,
		})
	}
	return out, nil
}

func logJSON(level, message string, fields map[string]interface{}) {
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
