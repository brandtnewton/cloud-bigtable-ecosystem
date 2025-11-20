package use_translator

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type UseTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func (t *UseTranslator) Translate(query *types.RawQuery, _ types.Keyspace, _ bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	use := query.Parser().Use_()
	if use == nil {
		return nil, nil, fmt.Errorf("failed to parse USE statement")
	}

	if use.Keyspace() == nil {
		return nil, nil, fmt.Errorf("missing keyspace in USE statement")
	}

	keyspace := types.Keyspace(use.Keyspace().GetText())

	// validate keyspace
	_, err := t.schemaMappingConfig.GetKeyspace(keyspace)
	if err != nil {
		return nil, nil, err
	}

	return nil, types.NewUseTableStatementMap(keyspace, query.RawCql()), nil
}

func (t *UseTranslator) Bind(st types.IPreparedQuery, values []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	panic("cannot bind USE statement")
}

func (t *UseTranslator) QueryType() types.QueryType {
	return types.QueryTypeUse
}

func NewUseTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &UseTranslator{schemaMappingConfig: schemaMappingConfig}
}
