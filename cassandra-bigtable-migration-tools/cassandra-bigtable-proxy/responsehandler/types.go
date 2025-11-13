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
package responsehandler

import (
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

type Metadata = *btpb.ResultSetMetadata

type ErrorDetail struct {
	TableName   string
	Keyspace    string
	Query       string
	ErrorString string
	ErrorState  string
}

type TypeHandler struct {
	Logger              *zap.Logger
	SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	ColumnMetadataCache map[string]map[string]message.ColumnMetadata
}

type Maptype struct {
	Key   string
	Value interface{}
}
