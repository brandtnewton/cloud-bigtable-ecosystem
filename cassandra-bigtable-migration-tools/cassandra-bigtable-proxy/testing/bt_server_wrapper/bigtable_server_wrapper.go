package bt_server_wrapper

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/bigtable/bttest"
	"context"
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

type BigtableTestServer struct {
	config             types.BigtableConfig
	conn               *grpc.ClientConn
	lastCreateTableReq *adminpb.CreateTableRequest
	server             *bttest.Server
	clients            *types.BigtableClientManager
}

func (b *BigtableTestServer) LastCreateTableReq() *adminpb.CreateTableRequest {
	return b.lastCreateTableReq
}

func (b *BigtableTestServer) Clients() *types.BigtableClientManager {
	return b.clients
}

func NewBigtableTestServer(config types.BigtableConfig) *BigtableTestServer {
	return &BigtableTestServer{config: config}
}

func (b *BigtableTestServer) SetUp(port int) {
	btt, err := bttest.NewServer(fmt.Sprintf("localhost:%d", port))
	if err != nil {
		fmt.Printf("Failed to setup server: %v", err)
		os.Exit(1)
	}
	b.conn, err = grpc.NewClient(btt.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(b.interceptCreateTableRequests))
	if err != nil {
		fmt.Printf("Failed to setup grpc: %v", err)
		os.Exit(1)
	}
	b.server = btt

	err = b.setupClientManager(b.conn)
	if err != nil {
		fmt.Printf("Failed to setup clients: %v", err)
		os.Exit(1)
	}
}

func (b *BigtableTestServer) setupClientManager(conn *grpc.ClientConn) error {
	ctx := context.Background()
	clients := make(map[types.Keyspace]*types.BigtableClientSet)
	for keyspace, instanceMapping := range b.config.Instances {
		client, err := bigtable.NewClient(ctx, "project", string(instanceMapping.InstanceId), option.WithGRPCConn(conn))
		if err != nil {
			fmt.Printf("Failed to create Bigtable client: %v", err)
			return err
		}
		adminClient, err := bigtable.NewAdminClient(ctx, "project", string(instanceMapping.InstanceId), option.WithGRPCConn(conn))
		if err != nil {
			fmt.Printf("Failed to create Bigtable admin client: %v", err)
			return err
		}
		clients[keyspace] = types.NewBigtableClientSet(adminClient, client)
	}

	b.clients = types.NewBigtableClientManager(clients)
	return nil
}

func (b *BigtableTestServer) Close() {
	if b.server != nil {
		b.server.Close()
	}
}

// interceptCreateTableRequests intercepts create table requests, so we can perform extra assertions on them
func (b *BigtableTestServer) interceptCreateTableRequests(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if method == "/google.bigtable.admin.v2.BigtableTableAdmin/CreateTable" {
		ctr, ok := req.(*adminpb.CreateTableRequest)
		if ok {
			b.lastCreateTableReq = ctr
		} else {
			b.lastCreateTableReq = nil
			return errors.New("unary interceptor: unexpected request type for CreateTable")
		}
	}

	err := invoker(ctx, method, req, reply, cc, opts...) // Invoke the actual RPC

	// Post-RPC logic (e.g., error handling, metrics)
	if err != nil {
		fmt.Printf("Unary call to %s failed: %v\n", method, err)
	}
	return err
}
