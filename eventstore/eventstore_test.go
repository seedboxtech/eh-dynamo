package dynamodb

import (
	"context"
	"os"
	"testing"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/eventstore"
)

func TestEventStore(t *testing.T) {
	// Local DynamoDb testing with Docker
	url := os.Getenv("DYNAMODB_HOST")
	if url == "" {
		url = "localhost:8000"
	}
	url = "http://" + url

	// These must be set for testing, even when using the mocked server.
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	if awsAccessKeyID == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "fakeMyKeyId")
	}
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsSecretAccessKey == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "fakeSecretAccessKey")
	}

	config := &EventStoreConfig{
		TablePrefix: "eventhorizonTest_" + eh.NewUUID().String(),
		Region:      "eu-west-1",
		Endpoint:    url,
	}
	store, err := NewEventStore(config)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}

	ctx := eh.NewContextWithNamespace(context.Background(), "ns")

	t.Log("creating tables for:", config.TablePrefix)
	if err := store.CreateTable(context.Background()); err != nil {
		t.Fatal("could not create table:", err)
	}
	if err := store.CreateTable(ctx); err != nil {
		t.Fatal("could not create table:", err)
	}

	defer func() {
		t.Log("deleting tables for:", config.TablePrefix)
		if err := store.DeleteTable(context.Background()); err != nil {
			t.Fatal("could not delete table: ", err)
		}
		if err := store.DeleteTable(ctx); err != nil {
			t.Fatal("could not delete table: ", err)
		}
	}()

	// Run the actual test suite.

	t.Log("event store with default namespace")
	eventstore.AcceptanceTest(t, context.Background(), store)

	t.Log("event store with other namespace")
	eventstore.AcceptanceTest(t, ctx, store)

	t.Log("event store maintainer")
	eventstore.MaintainerAcceptanceTest(t, context.Background(), store)
}
