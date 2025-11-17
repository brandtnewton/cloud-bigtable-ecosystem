/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package bigtableclient

import (
	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
	"go.uber.org/zap"
)

type BulkOperationResponse struct {
	FailedRows string
}

type BigtableClient struct {
	Clients             map[string]*bigtable.Client
	AdminClients        map[string]*bigtable.AdminClient
	Logger              *zap.Logger
	SqlClient           btpb.BigtableClient
	BigtableConfig      *types.BigtableConfig
	SchemaMappingConfig *schemaMapping.SchemaMappingConfig
}

type BigtableBulkMutation struct {
	mutations map[types.TableName][]translator.IBigtableMutation
}

func (b *BigtableBulkMutation) Mutations() map[types.TableName][]translator.IBigtableMutation {
	return b.mutations
}

func (b *BigtableBulkMutation) AddMutation(mut translator.IBigtableMutation) {
	if b.mutations[mut.Table()] == nil {
		b.mutations[mut.Table()] = []translator.IBigtableMutation{}
	}
	b.mutations[mut.Table()] = append(b.mutations[mut.Table()], mut)
}
