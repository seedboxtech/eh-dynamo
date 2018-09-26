package dynamodb

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"

	eh "github.com/looplab/eventhorizon"
)

// ErrCouldNotDialDB is when the database could not be dialed.
var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

// RepoConfig is a config for the DynamoDB event store.
type RepoConfig struct {
	TableName string
	Region    string
	Endpoint  string
}

func (c *RepoConfig) provideDefaults() {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
}

// Repo implements a DynamoDB repository for entities.
type Repo struct {
	service   *dynamo.DB
	config    *RepoConfig
	factoryFn func() eh.Entity
}

// NewRepo creates a new Repo.
func NewRepo(config *RepoConfig) (*Repo, error) {
	config.provideDefaults()
	awsConfig := &aws.Config{
		Region:   aws.String(config.Region),
		Endpoint: aws.String(config.Endpoint),
	}

	sess, err := session.NewSession(awsConfig)
	db := dynamo.New(sess)

	if err != nil {
		return nil, ErrCouldNotDialDB
	}

	return &Repo{
		service: db,
		config:  config,
	}, nil
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() eh.ReadRepo {
	return nil
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo) Find(ctx context.Context, id eh.UUID) (eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	table := r.service.Table(r.config.TableName)
	entity := r.factoryFn()

	err := table.Get("ID", id.String()).Consistent(true).One(entity)

	if err != nil {
		return nil, eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return entity, nil
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	table := r.service.Table(r.config.TableName)

	iter := table.Scan().Consistent(true).Iter()
	result := []eh.Entity{}
	entity := r.factoryFn()
	for iter.Next(entity) {
		result = append(result, entity)
		entity = r.factoryFn()
	}

	return result, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(ctx context.Context, entity eh.Entity) error {
	table := r.service.Table(r.config.TableName)

	if entity.EntityID() == eh.UUID("") {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   eh.ErrMissingEntityID,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	if err := table.Put(entity).Run(); err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(ctx context.Context, id eh.UUID) error {
	table := r.service.Table(r.config.TableName)

	if err := table.Delete("ID", id.String()).Run(); err != nil {
		return eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eh.Entity) {
	r.factoryFn = f
}
