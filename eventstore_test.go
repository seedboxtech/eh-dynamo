// Copyright (c) 2018 - The Event Horizon DynamoDB authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dynamodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/looplab/eventhorizon/mocks"

	"github.com/looplab/eventhorizon/eventstore"

	eh "github.com/looplab/eventhorizon"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/suite"
)

type EventStoreTestSuite struct {
	suite.Suite
	ctx   context.Context
	store *EventStore
}

// SetupTestSuite will create the store and dynamo table
func (suite *EventStoreTestSuite) SetupTest() {
	config := &EventStoreConfig{Endpoint: os.Getenv("DYNAMODB_HOST")}

	var err error
	suite.store, err = NewEventStore(config)
	assert.Nil(suite.T(), err, "there should be no error")
	assert.NotNil(suite.T(), suite.store, "there should be a store")

	suite.ctx = eh.NewContextWithNamespace(context.Background(), "ns")

	assert.Nil(suite.T(), suite.store.CreateTable(context.Background()), "could not create table")
	assert.Nil(suite.T(), suite.store.CreateTable(suite.ctx), "could not create table")
}

// TearDownTestSuite will delete the dynamo table
func (suite *EventStoreTestSuite) TearDownTest() {
	assert.Nil(suite.T(), suite.store.DeleteTable(context.Background()), "could not delete table")
	assert.Nil(suite.T(), suite.store.DeleteTable(suite.ctx), "could not delete table")
}

// TestEventStore will run all the acceptance tests for event stores
func (suite *EventStoreTestSuite) TestEventStore() {
	suite.T().Log("event store with default namespace")
	eventstore.AcceptanceTest(suite.T(), context.Background(), suite.store)

	suite.T().Log("event store with other namespace")
	eventstore.AcceptanceTest(suite.T(), suite.ctx, suite.store)

	suite.T().Log("event store maintainer")
	eventstore.MaintainerAcceptanceTest(suite.T(), context.Background(), suite.store)
}

// TestLoadAll will save a bunch of events and try to load them all from the event store
func (suite *EventStoreTestSuite) TestLoadAll() {
	id, _ := uuid.Parse("c1138e5f-f6fb-4dd0-8e79-255c6c8d3756")
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	expectedEvents := []eh.Event{
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "event1"},
			timestamp, mocks.AggregateType, id, 1),
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "event2"},
			timestamp, mocks.AggregateType, id, 2),
	}

	_ = suite.store.Save(context.Background(), expectedEvents, 0)

	events, err := suite.store.LoadAll(context.Background())
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), events, 2)

	for i, event := range events {
		if err := mocks.CompareEvents(event, expectedEvents[i]); err != nil {
			suite.T().Error("the event was incorrect:", err)
		}
		if event.Version() != i+1 {
			suite.T().Error("the event version should be correct:", event, event.Version())
		}
	}
}

// TestSaveInvalidAggregateId will save an aggregate with an invalid event aggregate ID
func (suite *EventStoreTestSuite) TestSaveInvalidAggregateId() {
	id, _ := uuid.Parse("c1138e5f-f6fb-4dd0-8e79-255c6c8d3756")
	id2, _ := uuid.Parse("c1138e5f-f6fb-4dd0-8e79-zzzzzzzzzz")
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	expectedEvents := []eh.Event{
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "event1"},
			timestamp, mocks.AggregateType, id, 1),
		eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "event1"},
			timestamp, mocks.AggregateType, id2, 1),
	}

	err := suite.store.Save(context.Background(), expectedEvents, 0)
	assert.EqualError(suite.T(), err, "invalid event (default)")
}

// TestEventStoreTestSuite starts the test suite
func TestEventStoreTestSuite(t *testing.T) {
	suite.Run(t, new(EventStoreTestSuite))
}
