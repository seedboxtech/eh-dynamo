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

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/suite"

	"github.com/stretchr/testify/assert"

	"github.com/guregu/dynamo"
	eh "github.com/looplab/eventhorizon"
)

// RepoTestSuite is intended to store values shared by multiple test and manage the setup/teardown
type RepoTestSuite struct {
	suite.Suite
	db   *dynamo.DB
	conf *RepoConfig
	repo *Repo
}

// SetupSuite will be run once, at the very start of the testing suite
func (suite *RepoTestSuite) SetupSuite() {
	conf := suite.getRepoConfig()
	db := suite.getDynamoDB(conf)

	suite.conf = conf
	suite.db = db
	suite.repo = suite.getRepo(suite.conf)
}

func (suite *RepoTestSuite) getDynamoDB(conf *RepoConfig) *dynamo.DB {
	awsConf := &aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: aws.String(conf.Endpoint),
	}
	awsSession, err := session.NewSession(awsConf)
	if err != nil {
		suite.T().Fatal("error setting up DB:", err)
	}
	return dynamo.New(awsSession)
}

func (suite *RepoTestSuite) getRepo(conf *RepoConfig) *Repo {
	testModel := &TestModel{}

	if err := suite.db.CreateTable(suite.conf.TableName, testModel).OnDemand(true).Run(); err != nil {
		suite.T().Fatal("could not create table:", err)
	}

	repo, err := NewRepo(conf)
	if err != nil || repo == nil {
		suite.T().Fatal("error creating repo:", err)
	}
	return repo
}

func (suite *RepoTestSuite) getRepoConfig() *RepoConfig {
	return &RepoConfig{
		TableName: "eventhorizonTest_" + uuid.New().String(),
		Endpoint:  os.Getenv("DYNAMODB_HOST"),
	}
}

func (suite *RepoTestSuite) BeforeTest(suiteName, testName string) {
	suite.repo.SetEntityFactory(func() eh.Entity { return &TestModel{} })
}

func (suite *RepoTestSuite) AfterTest(suiteName, testName string) {
	// Emptying the table
	entities, _ := suite.repo.FindAll(context.Background())
	for _, entity := range entities {
		_ = suite.repo.Remove(context.Background(), entity.EntityID())
	}
}

func (suite *RepoTestSuite) TestSaveAndFind() {
	testModel := &TestModel{ID: uuid.New(), Content: "test"}

	err := suite.repo.Save(context.Background(), testModel)
	if err != nil {
		suite.T().Fatal("error saving entity:", err)
	}

	result, err := suite.repo.Find(context.Background(), testModel.ID)
	if err != nil {
		suite.T().Fatal("error finding entity:", err)
	}
	assert.Equal(suite.T(), testModel.ID, result.EntityID())
}

func (suite *RepoTestSuite) TestSaveAndFindAll() {
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "test"})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "test2"})

	results, err := suite.repo.FindAll(context.Background())
	if err != nil {
		suite.T().Fatal("error finding entity:", err)
	}
	assert.Equal(suite.T(), 2, len(results))
}

func (suite *RepoTestSuite) TestSaveAndFindWithFilter() {
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "test", FilterableID: 123})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "test2", FilterableID: 123})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "test3", FilterableID: 456})

	results, err := suite.repo.FindWithFilter(context.Background(), "FilterableID = ?", 123)
	if err != nil {
		suite.T().Fatal("error finding entities:", err)
	}
	assert.Equal(suite.T(), 2, len(results))
}

func (suite *RepoTestSuite) TestSaveAndFindUsingIndex() {
	index := dynamo.Index{
		Name:           "testIndex",
		HashKey:        "FilterableID",
		HashKeyType:    dynamo.NumberType,
		RangeKey:       "FilterableSortKey",
		RangeKeyType:   dynamo.StringType,
		ProjectionType: dynamodb.ProjectionTypeAll,
	}
	if _, err := suite.db.Table(suite.conf.TableName).UpdateTable().CreateIndex(index).OnDemand(true).Run(); err != nil {
		suite.T().Fatal("could not create index:", err)
	}
	defer suite.db.Table(suite.conf.TableName).UpdateTable().DeleteIndex(index.Name).Run()

	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "testContent", FilterableID: 123, FilterableSortKey: "test"})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "testContent", FilterableID: 123, FilterableSortKey: "test"})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "testContent2", FilterableID: 123, FilterableSortKey: "test"})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "testContent", FilterableID: 456, FilterableSortKey: "test"})
	_ = suite.repo.Save(context.Background(), &TestModel{ID: uuid.New(), Content: "testContent", FilterableID: 123, FilterableSortKey: "test2"})

	indexInput := IndexInput{
		IndexName:         index.Name,
		PartitionKey:      index.HashKey,
		PartitionKeyValue: 123,
		SortKey:           index.RangeKey,
		SortKeyValue:      "test",
	}

	results, err := suite.repo.FindWithFilterUsingIndex(context.Background(), indexInput, "Content = ?", "testContent")
	if err != nil {
		suite.T().Fatal("error finding entities:", err)
	}
	assert.Equal(suite.T(), 2, len(results))

}

func (suite *RepoTestSuite) TestRemove() {
	testModel := &TestModel{ID: uuid.New(), Content: "test"}

	_ = suite.repo.Save(context.Background(), testModel)
	err := suite.repo.Remove(context.Background(), testModel.ID)
	if err != nil {
		suite.T().Fatal("failed to remove entity:", err)
	}

	result, err := suite.repo.Find(context.Background(), testModel.ID)
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != eh.ErrEntityNotFound || result != nil {
		suite.T().Fatal("entity should've been removed:", err)
	}
}

func (suite *RepoTestSuite) TestNoFactoryFn() {
	suite.repo.SetEntityFactory(nil)
	result, err := suite.repo.Find(context.Background(), uuid.New())
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != ErrModelNotSet || result != nil {
		suite.T().Fatal("an error should have occurred")
	}

	results, err := suite.repo.FindAll(context.Background())
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != ErrModelNotSet || results != nil {
		suite.T().Fatal("an error should have occurred")
	}
}

func (suite *RepoTestSuite) TestEmptyUUID() {
	err := suite.repo.Save(context.Background(), &TestModel{Content: "test"})
	assert.EqualError(suite.T(), err, "could not save entity: missing entity ID (default)")
}

func (suite *RepoTestSuite) TestParent() {
	result := suite.repo.Parent()
	assert.Nil(suite.T(), result)
}

type TestModel struct {
	ID                uuid.UUID `dynamo:",hash"`
	Content           string
	FilterableID      int
	FilterableSortKey string
}

// EntityID implements the EntityID method of the eventhorizon.Entity interface.
func (m *TestModel) EntityID() uuid.UUID {
	return m.ID
}

// TestRepoTestSuite is to make sure 'go test' runs this suite
func TestRepoTestSuite(t *testing.T) {
	suite.Run(t, new(RepoTestSuite))
}
