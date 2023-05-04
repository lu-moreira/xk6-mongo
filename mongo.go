package mongo

import (
	"context"
	"time"

	"go.k6.io/k6/js/modules"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func init() {
	modules.Register("k6/x/mongo", new(RootModule))
}

type (
	// RootModule is the global module instance that will create module
	// instances for each VU.
	RootModule struct{}

	// Client holds the mongo client
	Client struct {
		vu     modules.VU
		client *mongo.Client
	}

	// ModuleInstance represents an instance of the JS module.
	ModuleInstance struct {
		// vu provides methods for accessing internal k6 objects for a VU
		vu modules.VU
		// comparator is the exported type
		client *Client
	}
)

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

// NewModuleInstance implements the modules.Module interface returning a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{
		vu:     vu,
		client: &Client{vu: vu},
	}
}

// Exports implements the modules.Instance interface and returns the exported types for the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{
		Default: mi.client,
	}
}

func (m *Client) Connect(ctx context.Context, uri string) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	if err := client.Ping(context.Background(), readpref.Nearest()); err != nil {
		return err
	}

	m.client = client
	return err
}

type (
	AggregateResponse struct {
		Results  []bson.M
		Duration time.Duration
	}
)

func (m *Client) Aggregate(ctx context.Context, database, collection string, stages bson.D) (*AggregateResponse, error) {
	st := time.Now()
	cur, err := m.client.Database(database).Collection(collection).Aggregate(ctx, mongo.Pipeline{stages})
	if err != nil {
		return nil, err
	}

	var res []bson.M
	if err := cur.All(ctx, &res); err != nil {
		return nil, err
	}

	return &AggregateResponse{
		Results:  res,
		Duration: time.Since(st),
	}, nil
}
