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

func makeFunctionStatus(name, namespace string, annotations *map[string]string) types.FunctionStatus {
	return types.FunctionStatus{
		Name:              name,
		Image:             "docker:image",
		InvocationCount:   0,
		Replicas:          1,
		EnvProcess:        "",
		AvailableReplicas: 1,
		Labels:            nil,
		Annotations:       annotations,
		Namespace:         namespace,
	}
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
		makeFunctionStatus("biller", "faas", &annotations),
		makeFunctionStatus("secrter", "faas", &annotations),
	}

	fnTestNs := []types.FunctionStatus{
		makeFunctionStatus("transporter", "test", &annotations),
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
		makeFunctionStatus("function-name", "faas", &annotations),
		makeFunctionStatus("wrencher", "faas", &annotations),
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

		err := cacher.Invoke(TOPIC, makeInvocation(TOPIC))

		assert.NoError(t, err, "should not throw")
		clientMock.AssertNumberOfCalls(t, "InvokeSync", 3)
		clientMock.AssertExpectations(t)
	})

	t.Run("Should abort invocation of functions on receiving first error further returning it", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("InvokeSync", mock.Anything, mock.Anything, mock.Anything).Return([]byte{}, 500, errors.New("failed"))
		clientMock.On("InvokeAsync", mock.Anything, mock.Anything, mock.Anything).Return(false, 500, errors.New("async failed"))

		cacher := NewController(nil, clientMock, cacheMock)

		err := cacher.Invoke(TOPIC, makeInvocation(TOPIC))

		assert.Error(t, err, "failed")
	})

	t.Run("Should not invoke if there is no function for specified Topic", func(t *testing.T) {
		clientMock := new(MockOpenFaaSClient)
		clientMock.On("InvokeSync", mock.Anything, mock.Anything, mock.Anything).Return([]byte{}, 200, nil)

		cacher := NewController(nil, clientMock, cacheMock)

		err := cacher.Invoke("Security", makeInvocation("Security"))

		assert.NoError(t, err, "should not throw")
		clientMock.AssertNotCalled(t, "InvokeSync")
	})
}

// --- Helpers ---

func makeInvocation(topic string) *types2.OpenFaaSInvocation {
	message := []byte(`{"test": true}`)
	return &types2.OpenFaaSInvocation{
		Topic:       topic,
		Message:     &message,
		ContentType: "application/json",
	}
}

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

	tests := []struct {
		name       string
		condition  string
		payload    map[string]interface{}
		wantResult bool
		wantExists bool
	}{
		{"equality true", `status == "active"`, payload, true, true},
		{"equality false", `status == "inactive"`, payload, false, true},
		{"inequality true", `status != "inactive"`, payload, true, true},
		{"inequality false", `status != "active"`, payload, false, true},
		{"greater than true", `quantity > 10`, payload, true, true},
		{"greater than false", `quantity > 20`, payload, false, true},
		{"less than true", `quantity < 20`, payload, true, true},
		{"greater than or equal", `quantity >= 15`, payload, true, true},
		{"less than or equal", `quantity <= 15`, payload, true, true},
		{"Contains true", `name.Contains("Gonzalo")`, payload, true, true},
		{"Contains false", `name.Contains("Xavier")`, payload, false, true},
		{"missing key", `missing == "value"`, payload, false, false},
		{"value containing == characters", `ref == "a==b"`, map[string]interface{}{"ref": "a==b"}, true, true},
		{"value containing > character", `desc == "amount>100"`, map[string]interface{}{"desc": "amount>100"}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, exists := evaluateCondition(tt.condition, tt.payload)
			assert.Equal(t, tt.wantExists, exists)
			assert.Equal(t, tt.wantResult, result)
		})
	}
}

func TestApplyAllFilters(t *testing.T) {
	t.Parallel()

	controller := NewController(nil, nil, nil)

	payload := map[string]interface{}{
		"status": "active",
		"type":   "billing",
		"amount": float64(100),
	}

	realPayload := map[string]interface{}{
		"id": "ke_12345",
		"metadata": map[string]interface{}{
			"state": "done",
		},
	}

	pendingPayload := map[string]interface{}{
		"id": "ke_12345",
		"metadata": map[string]interface{}{
			"state": "pending",
		},
	}

	orPayload := map[string]interface{}{
		"id": "ug_99999",
		"metadata": map[string]interface{}{
			"state": "done",
		},
	}

	tests := []struct {
		name    string
		filter  string
		payload map[string]interface{}
		want    bool
	}{
		{"no filter set", "", payload, true},
		{"single equality condition", `status == "active"`, payload, true},
		{"AND conditions both true", `status == "active" && type == "billing"`, payload, true},
		{"AND conditions one false", `status == "active" && type == "shipping"`, payload, false},
		{"OR conditions first true", `status == "active" || type == "shipping"`, payload, true},
		{"OR conditions second true", `status == "inactive" || type == "billing"`, payload, true},
		{"OR conditions both false", `status == "inactive" || type == "shipping"`, payload, false},
		{"mixed AND/OR -- OR of AND groups", `status == "inactive" && type == "billing" || status == "active" && amount >= 50`, payload, true},
		{"encoded AND operators", `status == "active" \u0026\u0026 type == "billing"`, payload, true},
		{"encoded OR operators", `status == "inactive" \u007C\u007C type == "billing"`, payload, true},
		{"mixed encoded AND and OR operators", `status == "inactive" \u0026\u0026 type == "billing" \u007C\u007C status == "active" \u0026\u0026 type == "billing"`, payload, true},
		{"missing key in AND", `status == "active" && missing == "value"`, payload, false},
		{"missing nested key", `missing.nested.key == "value"`, payload, false},
		{"Contains AND equality on nested key", `id.Contains("ke_") && metadata.state == "done"`, realPayload, true},
		{"Contains AND equality with escaped quotes", `id.Contains("ke_") && metadata.state == \"done\"`, realPayload, true},
		{"Contains AND equality with encoded AND operator", `id.Contains("ke_") \u0026\u0026 metadata.state == \"done\"`, realPayload, true},
		{"Contains AND equality when one condition fails", `id.Contains("ke_") && metadata.state == "done"`, pendingPayload, false},
		{"Contains OR equality", `id.Contains("ke_") || metadata.state == "done"`, orPayload, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := controller.applyAllFilters(tt.filter, makeMessage(tt.payload))
			assert.Equal(t, tt.want, result)
		})
	}

	t.Run("invalid JSON message", func(t *testing.T) {
		badMsg := []byte("not json")
		result := controller.applyAllFilters(`status == "active"`, &badMsg)
		assert.False(t, result)
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
