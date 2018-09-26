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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

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

// SetupSuite will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (suite *RepoTestSuite) SetupSuite() {
	conf := suite.getRepoConfig()
	db := suite.getDynamoDB(conf)

	suite.conf = conf
	suite.db = db
}

func (suite *RepoTestSuite) getDynamoDB(conf *RepoConfig) *dynamo.DB {
	// These must be set for testing, even when using the mocked server.
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	if awsAccessKeyID == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "fakeMyKeyIds")
	}
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsSecretAccessKey == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "fakeSecretAccessKey")
	}
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
	repo, err := NewRepo(conf)
	if err != nil || repo == nil {
		suite.T().Fatal("error creating repo:", err)
	}
	repo.SetEntityFactory(func() eh.Entity { return &TestModel{} })
	return repo
}

func (suite *RepoTestSuite) getRepoConfig() *RepoConfig {
	// Local DynamoDb testing with Docker
	url := os.Getenv("DYNAMODB_HOST")
	if url == "" {
		url = "localhost:8000"
	}

	return &RepoConfig{
		TableName: "eventhorizonTest_" + eh.NewUUID().String(),
		Endpoint:  "http://" + url,
	}
}
func (suite *RepoTestSuite) BeforeTest(suiteName, testName string) {
	testModel := &TestModel{}
	if err := suite.db.CreateTable(suite.conf.TableName, testModel).Run(); err != nil {
		suite.T().Fatal("could not create table:", err)
	}

	suite.repo = suite.getRepo(suite.conf)
}

func (suite *RepoTestSuite) AfterTest(suiteName, testName string) {
	if err := suite.db.Table(suite.conf.TableName).DeleteTable().Run(); err != nil {
		suite.T().Fatal("could not delete table: ", err)
	}
}

func (suite *RepoTestSuite) TestSaveAndFind() {
	testModel := &TestModel{ID: eh.NewUUID(), Content: "test"}

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
	testModel := &TestModel{ID: eh.NewUUID(), Content: "test"}
	testModel2 := &TestModel{ID: eh.NewUUID(), Content: "test2"}

	err := suite.repo.Save(context.Background(), testModel)
	suite.repo.Save(context.Background(), testModel2)

	results, err := suite.repo.FindAll(context.Background())
	if err != nil {
		suite.T().Fatal("error finding entity:", err)
	}
	assert.Equal(suite.T(), 2, len(results))
}

func (suite *RepoTestSuite) TestRemove() {
	testModel := &TestModel{ID: eh.NewUUID(), Content: "test"}

	suite.repo.Save(context.Background(), testModel)
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
	result, err := suite.repo.Find(context.Background(), eh.NewUUID())
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != ErrModelNotSet || result != nil {
		suite.T().Fatal("an error should have occured:", err)
	}
}

type TestModel struct {
	ID      eh.UUID `dynamo:",hash"`
	Content string
}

// EntityID implements the EntityID method of the eventhorizon.Entity interface.
func (m *TestModel) EntityID() eh.UUID {
	return m.ID
}

// TestRepoTestSuite is to make sure 'go test' runs this suite
func TestRepoTestSuite(t *testing.T) {
	suite.Run(t, new(RepoTestSuite))
}
