/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Templum/rabbitmq-connector/pkg/config"
	"github.com/Templum/rabbitmq-connector/pkg/connector"
	"github.com/Templum/rabbitmq-connector/pkg/openfaas"
	"github.com/Templum/rabbitmq-connector/pkg/rabbitmq"
	"github.com/Templum/rabbitmq-connector/pkg/types"
	"github.com/Templum/rabbitmq-connector/pkg/version"
	"github.com/spf13/afero"

	_ "go.uber.org/automaxprocs"
)

// LogJSON is a helper function to log messages in JSON format
func LogJSON(level, message string, fields map[string]interface{}) {
	logData := map[string]interface{}{
		"@t":          time.Now().UTC().Format(time.RFC3339),
		"@m":          message,
		"@l":          level,
		"Application": "rabbitmq-connector",
	}
	for k, v := range fields {
		logData[k] = v
	}
	logJSON, _ := json.Marshal(logData)
	log.Println(string(logJSON))
}

func main() {
	log.SetFlags(0) // Disable the default timestamp

	commit, tag := version.GetReleaseInfo()
	LogJSON("info", "OpenFaaS RabbitMQ Connector started", map[string]interface{}{
		"Version": tag,
		"Commit":  commit,
	})

	if rawValue, ok := os.LookupEnv("basic_auth"); ok {
		active, _ := strconv.ParseBool(rawValue)
		if path, ok := os.LookupEnv("secret_mount_path"); ok && active {
			LogJSON("info", "Will read basic64 secret from path", map[string]interface{}{
				"path": path,
			})
		}
	}

	// Building our Config from envs
	conf, validationErr := config.NewConfig(afero.NewOsFs())
	if validationErr != nil {
		LogJSON("fatal", "During Config validation error occurred", map[string]interface{}{
			"error": validationErr.Error(),
		})
		os.Exit(1)
	}

	// Setup Application Context to ensure gracefully shutdowns
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpClient := types.MakeHTTPClient(conf.InsecureSkipVerify, conf.MaxClientsPerHost, 60*time.Second)
	// Setup OpenFaaS Controller which is used for querying and more
	ofSDK := openfaas.NewController(conf, openfaas.NewClient(httpClient, conf.BasicAuth, conf.GatewayURL), openfaas.NewTopicFunctionCache())
	go ofSDK.Start(ctx)
	LogJSON("info", "Started Cache Task which populates the topic map", nil)
	LogJSON("info", "Configured QoS settings", map[string]interface{}{
		"prefetch_count": conf.PrefetchCount,
		"prefetch_size":  conf.PrefetchSize,
		"global":         conf.PrefetchGlobal,
	})

	healthMetrics := &rabbitmq.OverallHealthMetrics{}
	c := connector.New(rabbitmq.NewConnectionManager(rabbitmq.NewBroker(), conf.TLSConfig), rabbitmq.NewFactory().WithHealthMetrics(healthMetrics).WithQoS(conf.PrefetchCount, conf.PrefetchSize, conf.PrefetchGlobal), ofSDK, conf)
	err := c.Run()

	if err != nil {
		LogJSON("fatal", "Received error during Connector starting", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	LogJSON("info", "Waiting for messages. To exit press CTRL+C", nil)

	sig := <-signalChannel
	switch sig {
	case os.Interrupt:
		LogJSON("info", "Received SIGINT preparing for shutdown", nil)
		c.Shutdown()
		cancel()
	case syscall.SIGTERM:
		LogJSON("info", "Received SIGTERM shutting down", nil)
		c.Shutdown()
		cancel()
	}
}
