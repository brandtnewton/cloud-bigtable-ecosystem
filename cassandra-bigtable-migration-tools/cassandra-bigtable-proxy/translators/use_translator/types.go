package use_translator

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type UseTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *UseTranslator) Translate(query *types.RawQuery, _ types.Keyspace) (types.IPreparedQuery, error) {
	use, err := query.Parser().Use_()
	if err != nil {
		return nil, err
	}

	keyspace, err := common.ParseKeyspace(use.Keyspace(), "")
	if err != nil {
		return nil, err
	}

	// validate keyspace
	_, err = t.schemaMappingConfig.GetKeyspace(keyspace)
	if err != nil {
		return nil, err
	}

	return types.NewUseTableStatementMap(keyspace, query.RawCql()), nil
}

func (t *UseTranslator) Bind(st types.IPreparedQuery, _ *types.QueryParameterValues, _ primitive.ProtocolVersion, _ int32, _ []byte) (types.IExecutableQuery, error) {
	use, ok := st.(*types.UseTableStatementMap)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	return use, nil
}

func (t *UseTranslator) QueryType() types.QueryType {
	return types.QueryTypeUse
}

func NewUseTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &UseTranslator{schemaMappingConfig: schemaMappingConfig}
}
