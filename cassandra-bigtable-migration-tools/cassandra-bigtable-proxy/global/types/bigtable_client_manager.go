package types

import (
	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"google.golang.org/api/transport/grpc"
	grpc_lib "google.golang.org/grpc"
)

type BigtableClientSet struct {
	admin     *bigtable.AdminClient
	data      *bigtable.Client
	sqlClient btpb.BigtableClient
}

func NewBigtableClientSet(admin *bigtable.AdminClient, data *bigtable.Client, sqlClient btpb.BigtableClient) *BigtableClientSet {
	return &BigtableClientSet{admin: admin, data: data, sqlClient: sqlClient}
}

type BigtableClientManager struct {
	clients map[Keyspace]*BigtableClientSet
}

func NewBigtableClientManager(clients map[Keyspace]*BigtableClientSet) *BigtableClientManager {
	return &BigtableClientManager{clients: clients}
}

func CreateBigtableClientManager(ctx context.Context, config *ProxyInstanceConfig) (*BigtableClientManager, error) {
	clients := make(map[Keyspace]*BigtableClientSet)
	for _, instanceConfig := range config.BigtableConfig.Instances {
		clientSet, err := createBigtableClientSet(ctx, config, instanceConfig)
		if err != nil {
			return nil, err
		}
		clients[instanceConfig.Keyspace] = clientSet
	}
	return &BigtableClientManager{clients: clients}, nil
}

func createBigtableClientSet(ctx context.Context, config *ProxyInstanceConfig, instanceMapping *InstanceMapping) (*BigtableClientSet, error) {
	adminClient, err := bigtable.NewAdminClient(ctx, config.BigtableConfig.ProjectID, string(instanceMapping.InstanceId), option.WithUserAgent(config.Options.UserAgent))
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client for keyspace `%s`: %v", instanceMapping.Keyspace, err)
	}
	// Create a gRPC connection pool with the specified number of channels
	pool := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 10)) // 10 MB max message size

	// Specify gRPC connection options, including your custom number of channels
	opts := []option.ClientOption{
		option.WithGRPCDialOption(pool),
		option.WithGRPCConnectionPool(config.BigtableConfig.Session.GrpcChannels),
		option.WithUserAgent(config.Options.UserAgent),
	}

	endpoint := "bigtable.googleapis.com:443"
	if config.BigtableConfig.AdminEndpoint != "" {
		endpoint = config.BigtableConfig.AdminEndpoint
	}

	conn, err := grpc.Dial(ctx, append(opts, option.WithEndpoint(endpoint))...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %v", err)
	}

	client, err := bigtable.NewClientWithConfig(ctx, config.BigtableConfig.ProjectID, string(instanceMapping.InstanceId), bigtable.ClientConfig{
		AppProfile: instanceMapping.AppProfileID,
	}, option.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create bigtable client for instance %s: %v", instanceMapping.InstanceId, err)
	}

	sqlClient := btpb.NewBigtableClient(conn)

	return NewBigtableClientSet(adminClient, client, sqlClient), nil
}

func (b *BigtableClientManager) GetTableClient(keyspace Keyspace, table TableName) (*bigtable.Table, error) {
	c, err := b.GetClient(keyspace)
	if err != nil {
		return nil, err
	}
	return c.Open(string(table)), nil
}
func (b *BigtableClientManager) getClientSet(keyspace Keyspace) (*BigtableClientSet, error) {
	client, ok := b.clients[keyspace]
	if !ok {
		return nil, fmt.Errorf("bigtable client not found for keyspace '%s'", keyspace)
	}
	return client, nil
}

func (b *BigtableClientManager) GetClient(keyspace Keyspace) (*bigtable.Client, error) {
	set, err := b.getClientSet(keyspace)
	if err != nil {
		return nil, err
	}
	return set.data, nil
}

func (b *BigtableClientManager) GetSqlClient(keyspace Keyspace) (btpb.BigtableClient, error) {
	set, err := b.getClientSet(keyspace)
	if err != nil {
		return nil, err
	}
	return set.sqlClient, nil
}

func (b *BigtableClientManager) GetAdmin(keyspace Keyspace) (*bigtable.AdminClient, error) {
	set, err := b.getClientSet(keyspace)
	if err != nil {
		return nil, err
	}
	return set.admin, nil
}

func (b *BigtableClientManager) Close() {
	for _, clients := range b.clients {
		_ = clients.admin.Close()
		_ = clients.data.Close()
	}
}
