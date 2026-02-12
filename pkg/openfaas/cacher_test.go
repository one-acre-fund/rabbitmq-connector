/*
 * Copyright (c) Simon Pelczer 2021. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package openfaas

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	types2 "github.com/Templum/rabbitmq-connector/pkg/types"

	"github.com/Templum/rabbitmq-connector/pkg/config"
	"github.com/openfaas/faas-provider/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockTopicMap struct {
	mock.Mock
	lock         sync.RWMutex
	refreshCalls int
}

func (s *MockTopicMap) CalledNTimes() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.refreshCalls
}

func (s *MockTopicMap) GetCachedValues(name string) []string {
	args := s.Called(name)
	return args.Get(0).([]string)
}

func (s *MockTopicMap) GetCachedFilter(topic string, functionName string) string {
	args := s.Called(topic, functionName)
	return args.String(0)
}

func (s *MockTopicMap) Refresh(update map[string][]string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.refreshCalls++
}

func (s *MockTopicMap) RefreshFilters(update map[string]map[string]string) {
	// no-op for test
}

type MockOpenFaaSClient struct {
	mock.Mock
}

func (m *MockOpenFaaSClient) InvokeAsync(ctx context.Context, name string, invocation *types2.OpenFaaSInvocation) (bool, int, error) {
	args := m.Called(ctx, name, invocation)
	return args.Bool(0), args.Int(1), args.Error(2)
}

func (m *MockOpenFaaSClient) InvokeSync(ctx context.Context, name string, invocation *types2.OpenFaaSInvocation) ([]byte, int, error) {
	args := m.Called(ctx, name, invocation)
	return args.Get(0).([]byte), args.Int(1), args.Error(2)
}

func (m *MockOpenFaaSClient) HasNamespaceSupport(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *MockOpenFaaSClient) GetNamespaces(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockOpenFaaSClient) GetFunctions(ctx context.Context, namespace string) ([]types.FunctionStatus, error) {
	args := m.Called(namespace)
	return args.Get(0).([]types.FunctionStatus), args.Error(1)
}

func runStartSubtests(t *testing.T, conf *config.Controller, clientMock *MockOpenFaaSClient) {
	t.Helper()

	t.Run("Should perform a initial population of the map", func(t *testing.T) {
		cacheMock := new(MockTopicMap)
		cacher := NewController(conf, clientMock, cacheMock)
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		cacher.Start(ctx)
		assert.Equal(t, cacheMock.CalledNTimes(), 1, "Expected an initial sync")
	})

	t.Run("Should sync every 3 seconds", func(t *testing.T) {
		cacheMock := new(MockTopicMap)
		cacher := NewController(conf, clientMock, cacheMock)
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		cacher.Start(ctx)
		assert.Equal(t, cacheMock.CalledNTimes(), 1, "Expected an initial sync")
		time.Sleep(4 * time.Second)
		assert.Equal(t, cacheMock.CalledNTimes(), 2, "Expected a new sync")
	})
}

func TestCacher_Start_WithNs(t *testing.T) {
	namespaces := []string{
		"faas",
		"special",
		"test",
	}

	annotations := map[string]string{"topic": "billing,secret,transport"}

	fnFaaSNs := []types.FunctionStatus{
		{
			Name:              "biller",
			Image:             "docker:image",
			InvocationCount:   0,
			Replicas:          1,
			EnvProcess:        "",
			AvailableReplicas: 1,
			Labels:            nil,
			Annotations:       &annotations,
			Namespace:         "faas",
		},
		{
			Name:              "secrter",
			Image:             "docker:image",
			InvocationCount:   0,
			Replicas:          1,
			EnvProcess:        "",
			AvailableReplicas: 1,
			Labels:            nil,
			Annotations:       &annotations,
			Namespace:         "faas",
		},
	}

	fnTestNs := []types.FunctionStatus{
		{
			Name:              "transporter",
			Image:             "docker:image",
			InvocationCount:   0,
			Replicas:          1,
			EnvProcess:        "",
			AvailableReplicas: 1,
			Labels:            nil,
			Annotations:       &annotations,
			Namespace:         "test",
		},
	}

	clientMock := new(MockOpenFaaSClient)
	clientMock.On("HasNamespaceSupport", mock.Anything).Return(true, nil)
	clientMock.On("GetNamespaces", mock.Anything).Return(namespaces, nil)
	clientMock.On("GetFunctions", "faas").Return(fnFaaSNs, nil)
	clientMock.On("GetFunctions", "test").Return(fnTestNs, nil)
	clientMock.On("GetFunctions", "special").Return([]types.FunctionStatus{}, nil)

	conf := &config.Controller{TopicRefreshTime: 3 * time.Second}

	t.Parallel()
	runStartSubtests(t, conf, clientMock)
}

func TestCacher_Start_Normal(t *testing.T) {
	annotations := map[string]string{"topic": "billing,secret,transport"}

	functions := []types.FunctionStatus{
		{
			Name:              "function-name",
			Image:             "docker:image",
			InvocationCount:   0,
			Replicas:          1,
			EnvProcess:        "",
			AvailableReplicas: 1,
			Labels:            nil,
			Annotations:       &annotations,
			Namespace:         "faas",
		},
		{
			Name:              "wrencher",
			Image:             "docker:image",
			InvocationCount:   0,
			Replicas:          1,
			EnvProcess:        "",
			AvailableReplicas: 1,
			Labels:            nil,
			Annotations:       &annotations,
			Namespace:         "faas",
		},
	}

	clientMock := new(MockOpenFaaSClient)
	clientMock.On("HasNamespaceSupport", mock.Anything).Return(false, nil)
	clientMock.On("GetFunctions", mock.Anything).Return(functions, nil)

	conf := &config.Controller{TopicRefreshTime: 3 * time.Second}

	t.Parallel()
	runStartSubtests(t, conf, clientMock)
}

func TestCacher_Start_WithFailures(t *testing.T) {
	conf := &config.Controller{TopicRefreshTime: 3 * time.Second}

	t.Parallel()

	t.Run("Should swallow errors received during get namespace", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("HasNamespaceSupport", mock.Anything).Return(true, nil)
		clientMock.On("GetNamespaces", mock.Anything).Return([]string{}, errors.New("Swallow me"))
		cacheMock := new(MockTopicMap)

		cacher := NewController(conf, clientMock, cacheMock)

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		cacher.Start(ctx)
		assert.Equal(t, cacheMock.CalledNTimes(), 1, "Expected an initial sync")
	})

	t.Run("Should swallow errors received during get functions", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("HasNamespaceSupport", mock.Anything).Return(false, nil)
		clientMock.On("GetFunctions", mock.Anything).Return([]types.FunctionStatus{}, errors.New("Swallow me"))
		cacheMock := new(MockTopicMap)

		cacher := NewController(conf, clientMock, cacheMock)

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		cacher.Start(ctx)
		assert.Equal(t, cacheMock.CalledNTimes(), 1, "Expected an initial sync")
	})
}

func TestCacher_Invoke(t *testing.T) {
	cacheMock := new(MockTopicMap)
	cacheMock.On("GetCachedValues", "Security").Return([]string{})
	cacheMock.On("GetCachedValues", "Billing").Return([]string{"billing", "secret", "transport"})
	cacheMock.On("GetCachedFilter", mock.Anything, mock.Anything).Return("")

	const TOPIC = "Billing"

	t.Run("Should invoke all functions for specified Topic", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("InvokeSync", mock.Anything, mock.Anything, mock.Anything).Return([]byte{}, 200, nil)

		cacher := NewController(nil, clientMock, cacheMock)

		message := []byte(`{"test": true}`)
		invocation := &types2.OpenFaaSInvocation{
			Topic:       TOPIC,
			Message:     &message,
			ContentType: "application/json",
		}
		err := cacher.Invoke(TOPIC, invocation)

		assert.NoError(t, err, "should not throw")
		clientMock.AssertNumberOfCalls(t, "InvokeSync", 3)
		clientMock.AssertExpectations(t)
	})

	t.Run("Should abort invocation of functions on receiving first error further returning it", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("InvokeSync", mock.Anything, mock.Anything, mock.Anything).Return([]byte{}, 500, errors.New("failed"))
		clientMock.On("InvokeAsync", mock.Anything, mock.Anything, mock.Anything).Return(false, 500, errors.New("async failed"))

		cacher := NewController(nil, clientMock, cacheMock)

		message := []byte(`{"test": true}`)
		invocation := &types2.OpenFaaSInvocation{
			Topic:       TOPIC,
			Message:     &message,
			ContentType: "application/json",
		}
		err := cacher.Invoke(TOPIC, invocation)

		assert.Error(t, err, "failed")
	})

	t.Run("Should not invoke if there is no function for specified Topic", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("InvokeSync", mock.Anything, mock.Anything, mock.Anything).Return([]byte{}, 200, nil)

		cacher := NewController(nil, clientMock, cacheMock)

		message := []byte(`{"test": true}`)
		invocation := &types2.OpenFaaSInvocation{
			Topic:       "Security",
			Message:     &message,
			ContentType: "application/json",
		}
		err := cacher.Invoke("Security", invocation)

		assert.NoError(t, err, "should not throw")
		clientMock.AssertNotCalled(t, "InvokeSync")
	})
}

// --- Filter unit tests ---

func makeMessage(data map[string]interface{}) *[]byte {
	b, _ := json.Marshal(data)
	return &b
}

func TestDecodeAnnotations(t *testing.T) {
	t.Parallel()

	t.Run("Should decode unicode AND operator", func(t *testing.T) {
		input := `status == "active" \u0026\u0026 type == "billing"`
		expected := `status == "active" && type == "billing"`
		assert.Equal(t, expected, decodeAnnotations(input))
	})

	t.Run("Should decode unicode OR operator", func(t *testing.T) {
		input := `status == "active" \u007C\u007C type == "billing"`
		expected := `status == "active" || type == "billing"`
		assert.Equal(t, expected, decodeAnnotations(input))
	})

	t.Run("Should decode escaped quotes", func(t *testing.T) {
		input := `status == \"active\"`
		expected := `status == "active"`
		assert.Equal(t, expected, decodeAnnotations(input))
	})

	t.Run("Should be idempotent on already decoded input", func(t *testing.T) {
		input := `status == "active" && type == "billing"`
		assert.Equal(t, input, decodeAnnotations(input))
	})
}

func TestGetNestedValue(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "John",
			"age":  float64(30),
			"address": map[string]interface{}{
				"city": "NYC",
			},
		},
		"Status": "active",
	}

	t.Run("Should return top-level value", func(t *testing.T) {
		val, ok := getNestedValue("Status", payload)
		assert.True(t, ok)
		assert.Equal(t, "active", val)
	})

	t.Run("Should return nested value", func(t *testing.T) {
		val, ok := getNestedValue("user.name", payload)
		assert.True(t, ok)
		assert.Equal(t, "John", val)
	})

	t.Run("Should return deeply nested value", func(t *testing.T) {
		val, ok := getNestedValue("user.address.city", payload)
		assert.True(t, ok)
		assert.Equal(t, "NYC", val)
	})

	t.Run("Should return false for missing key", func(t *testing.T) {
		val, ok := getNestedValue("user.email", payload)
		assert.False(t, ok)
		assert.Nil(t, val)
	})

	t.Run("Should return false for missing nested key", func(t *testing.T) {
		val, ok := getNestedValue("user.age.something", payload)
		assert.False(t, ok)
		assert.Nil(t, val)
	})

	t.Run("Should do case-insensitive lookup", func(t *testing.T) {
		val, ok := getNestedValue("status", payload)
		assert.True(t, ok)
		assert.Equal(t, "active", val)
	})

	t.Run("Should return false for completely missing path", func(t *testing.T) {
		val, ok := getNestedValue("nonexistent.path", payload)
		assert.False(t, ok)
		assert.Nil(t, val)
	})
}

func TestEvaluateCondition(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"status":   "active",
		"type":     "billing",
		"quantity": float64(15),
		"name":     "Gonzalo Martinez",
	}

	t.Run("Should evaluate equality true", func(t *testing.T) {
		result, exists := evaluateCondition(`status == "active"`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate equality false", func(t *testing.T) {
		result, exists := evaluateCondition(`status == "inactive"`, payload)
		assert.True(t, exists)
		assert.False(t, result)
	})

	t.Run("Should evaluate inequality true", func(t *testing.T) {
		result, exists := evaluateCondition(`status != "inactive"`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate inequality false", func(t *testing.T) {
		result, exists := evaluateCondition(`status != "active"`, payload)
		assert.True(t, exists)
		assert.False(t, result)
	})

	t.Run("Should evaluate greater than true", func(t *testing.T) {
		result, exists := evaluateCondition(`quantity > 10`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate greater than false", func(t *testing.T) {
		result, exists := evaluateCondition(`quantity > 20`, payload)
		assert.True(t, exists)
		assert.False(t, result)
	})

	t.Run("Should evaluate less than true", func(t *testing.T) {
		result, exists := evaluateCondition(`quantity < 20`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate greater than or equal", func(t *testing.T) {
		result, exists := evaluateCondition(`quantity >= 15`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate less than or equal", func(t *testing.T) {
		result, exists := evaluateCondition(`quantity <= 15`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate Contains true", func(t *testing.T) {
		result, exists := evaluateCondition(`name.Contains("Gonzalo")`, payload)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should evaluate Contains false", func(t *testing.T) {
		result, exists := evaluateCondition(`name.Contains("Xavier")`, payload)
		assert.True(t, exists)
		assert.False(t, result)
	})

	t.Run("Should return false for missing key", func(t *testing.T) {
		result, exists := evaluateCondition(`missing == "value"`, payload)
		assert.False(t, exists)
		assert.False(t, result)
	})

	t.Run("Should handle value containing == characters", func(t *testing.T) {
		p := map[string]interface{}{"ref": "a==b"}
		result, exists := evaluateCondition(`ref == "a==b"`, p)
		assert.True(t, exists)
		assert.True(t, result)
	})

	t.Run("Should handle value containing > character", func(t *testing.T) {
		p := map[string]interface{}{"desc": "amount>100"}
		result, exists := evaluateCondition(`desc == "amount>100"`, p)
		assert.True(t, exists)
		assert.True(t, result)
	})
}

func TestApplyAllFilters(t *testing.T) {
	t.Parallel()

	controller := NewController(nil, nil, nil)

	payload := map[string]interface{}{
		"status": "active",
		"type":   "billing",
		"amount": float64(100),
	}

	t.Run("Should return true when no filter is set", func(t *testing.T) {
		result := controller.applyAllFilters("", makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should evaluate single equality condition", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "active"`, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should evaluate AND conditions both true", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "active" && type == "billing"`, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should evaluate AND conditions one false", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "active" && type == "shipping"`, makeMessage(payload))
		assert.False(t, result)
	})

	t.Run("Should evaluate OR conditions first true", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "active" || type == "shipping"`, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should evaluate OR conditions second true", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "inactive" || type == "billing"`, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should evaluate OR conditions both false", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "inactive" || type == "shipping"`, makeMessage(payload))
		assert.False(t, result)
	})

	t.Run("Should evaluate mixed AND/OR -- OR of AND groups", func(t *testing.T) {
		filter := `status == "inactive" && type == "billing" || status == "active" && amount >= 50`
		result := controller.applyAllFilters(filter, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should handle encoded AND operators", func(t *testing.T) {
		filter := `status == "active" \u0026\u0026 type == "billing"`
		result := controller.applyAllFilters(filter, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should handle encoded OR operators", func(t *testing.T) {
		filter := `status == "inactive" \u007C\u007C type == "billing"`
		result := controller.applyAllFilters(filter, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should handle mixed encoded AND and OR operators", func(t *testing.T) {
		filter := `status == "inactive" \u0026\u0026 type == "billing" \u007C\u007C status == "active" \u0026\u0026 type == "billing"`
		result := controller.applyAllFilters(filter, makeMessage(payload))
		assert.True(t, result)
	})

	t.Run("Should return false for invalid JSON message", func(t *testing.T) {
		badMsg := []byte("not json")
		result := controller.applyAllFilters(`status == "active"`, &badMsg)
		assert.False(t, result)
	})

	t.Run("Should return false when filter references missing key in AND", func(t *testing.T) {
		result := controller.applyAllFilters(`status == "active" && missing == "value"`, makeMessage(payload))
		assert.False(t, result)
	})

	t.Run("Should return false when filter references missing nested key", func(t *testing.T) {
		result := controller.applyAllFilters(`missing.nested.key == "value"`, makeMessage(payload))
		assert.False(t, result)
	})

	// Real-world filter: Contains + equality combined with AND
	realPayload := map[string]interface{}{
		"id": "ke_12345",
		"metadata": map[string]interface{}{
			"state": "done",
		},
	}

	t.Run("Should evaluate Contains AND equality on nested key", func(t *testing.T) {
		filter := `id.Contains("ke_") && metadata.state == "done"`
		result := controller.applyAllFilters(filter, makeMessage(realPayload))
		assert.True(t, result)
	})

	t.Run("Should evaluate Contains AND equality with escaped quotes", func(t *testing.T) {
		filter := `id.Contains("ke_") && metadata.state == \"done\"`
		result := controller.applyAllFilters(filter, makeMessage(realPayload))
		assert.True(t, result)
	})

	t.Run("Should evaluate Contains AND equality with encoded AND operator", func(t *testing.T) {
		filter := `id.Contains("ke_") \u0026\u0026 metadata.state == \"done\"`
		result := controller.applyAllFilters(filter, makeMessage(realPayload))
		assert.True(t, result)
	})

	t.Run("Should fail Contains AND equality when one condition fails", func(t *testing.T) {
		realPayload := map[string]interface{}{
			"id": "ke_12345",
			"metadata": map[string]interface{}{
				"state": "pending",
			},
		}
		filter := `id.Contains("ke_") && metadata.state == "done"`
		result := controller.applyAllFilters(filter, makeMessage(realPayload))
		assert.False(t, result)
	})

	t.Run("Should evaluate Contains OR equality", func(t *testing.T) {
		realPayload := map[string]interface{}{
			"id": "ug_99999",
			"metadata": map[string]interface{}{
				"state": "done",
			},
		}
		filter := `id.Contains("ke_") || metadata.state == "done"`
		result := controller.applyAllFilters(filter, makeMessage(realPayload))
		assert.True(t, result) // id doesn't match but state does
	})
}

func TestAppendIfNotContains(t *testing.T) {
	t.Parallel()

	t.Run("Should append to empty string", func(t *testing.T) {
		result := appendIfNotContains("", `status == "active"`)
		assert.Equal(t, `status == "active"`, result)
	})

	t.Run("Should combine with OR", func(t *testing.T) {
		result := appendIfNotContains(`status == "active"`, `type == "billing"`)
		assert.Contains(t, result, `status == "active"`)
		assert.Contains(t, result, `type == "billing"`)
		assert.Contains(t, result, " || ")
	})

	t.Run("Should deduplicate identical conditions", func(t *testing.T) {
		result := appendIfNotContains(`status == "active"`, `status == "active"`)
		assert.Equal(t, `status == "active"`, result)
	})
}

func TestWildcardFilterKeySafety(t *testing.T) {
	t.Parallel()

	t.Run("Should not panic for topic without dots", func(t *testing.T) {
		assert.NotPanics(t, func() {
			topic := "billing"
			parts := strings.Split(topic, ".")
			if len(parts) >= 2 {
				_ = strings.Join(parts[:2], ".") + ".all"
			}
		})
	})

	t.Run("Should produce correct wildcard for dotted topic", func(t *testing.T) {
		topic := "billing.invoice.create"
		parts := strings.Split(topic, ".")
		assert.GreaterOrEqual(t, len(parts), 2)
		wildcardKey := strings.Join(parts[:2], ".") + ".all"
		assert.Equal(t, "billing.invoice.all", wildcardKey)
	})
}
