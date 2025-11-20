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

package translators

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/alter_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/create_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/delete_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/drop_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/insert_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/select_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/truncate_translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/update_translator"
	"go.uber.org/zap"
)

type Translator struct {
	Logger              *zap.Logger
	SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	// determines the encoding for int row keys in all new tables
	DefaultIntRowKeyEncoding types.IntRowKeyEncodingType
	translators              map[types.QueryType]types.IQueryTranslator
}

func NewTranslator(logger *zap.Logger, schemaMappingConfig *schemaMapping.SchemaMappingConfig, defaultIntRowKeyEncoding types.IntRowKeyEncodingType) *Translator {
	// add more translators here
	translators := []types.IQueryTranslator{
		select_translator.NewSelectTranslator(schemaMappingConfig),
		insert_translator.NewInsertTranslator(schemaMappingConfig),
		update_translator.NewUpdateTranslator(schemaMappingConfig),
		delete_translator.NewDeleteTranslator(schemaMappingConfig),
		truncate_translator.NewTruncateTranslator(schemaMappingConfig),
		create_translator.NewCreateTranslator(schemaMappingConfig, defaultIntRowKeyEncoding),
		alter_translator.NewAlterTranslator(schemaMappingConfig),
		drop_translator.NewDropTranslator(schemaMappingConfig),
	}

	var tm = make(map[types.QueryType]types.IQueryTranslator)
	for _, t := range translators {
		if _, has := tm[t.QueryType()]; has {
			panic(fmt.Errorf("translator of type %s already registered", t.QueryType().String()))
		}
		tm[t.QueryType()] = t
	}
	return &Translator{
		Logger:                   logger,
		SchemaMappingConfig:      schemaMappingConfig,
		DefaultIntRowKeyEncoding: defaultIntRowKeyEncoding,
		translators:              tm,
	}
}
