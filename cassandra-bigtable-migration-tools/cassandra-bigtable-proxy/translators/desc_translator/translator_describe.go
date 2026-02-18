package desc_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type DescTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *DescTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace) (types.IPreparedQuery, error) {
	s, err := query.Parser().DescribeStatement()
	if err != nil {
		return nil, err
	}

	d := s.DescribeTarget()

	var result types.IDescribeQueryVariant
	if d.DescribeTargetKeyspaces() != nil { // "DESC KEYSPACES;"
		result = types.NewDescribeKeyspacesQuery()
	} else if d.DescribeTargetTables() != nil { // "DESC TABLES;"
		result = types.NewDescribeTablesQuery()
	} else if d.DescribeTargetTable() != nil { // "DESC TABLE [$KEYSPACE.]$TABLE;"
		keyspace, table, err := common.ParseTableSpec(d.DescribeTargetTable().TableSpec(), sessionKeyspace)
		if err != nil {
			return nil, err
		}
		result = types.NewDescribeTableQuery(keyspace, table)
	} else if d.DescribeTargetKeyspace() != nil { // "DESC KEYSPACE $KEYSPACE;"
		keyspace, err := common.ParseKeyspace(d.DescribeTargetKeyspace().Keyspace(), sessionKeyspace)
		if err != nil {
			return nil, err
		}
		result = types.NewDescribeKeyspaceQuery(keyspace)
	} else {
		return nil, errors.New("failed to parse describe query")
	}
	return types.NewDescribeQuery(query.RawCql(), result), nil
}

func (t *DescTranslator) Bind(st types.IPreparedQuery, _ *types.QueryParameterValues, _ primitive.ProtocolVersion, _ int32, _ []byte) (types.IExecutableQuery, error) {
	desc, ok := st.(*types.DescribeQuery)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	return desc, nil
}

func (t *DescTranslator) QueryType() types.QueryType {
	return types.QueryTypeDescribe
}

func NewDescTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &DescTranslator{schemaMappingConfig: schemaMappingConfig}
}
