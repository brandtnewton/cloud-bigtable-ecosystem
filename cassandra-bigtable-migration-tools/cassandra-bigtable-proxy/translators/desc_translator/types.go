package desc_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type DescTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func (t *DescTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, _ bool) (types.IPreparedQuery, *types.QueryParameterValues, error) {
	s := query.Parser().DescribeStatement()
	if s == nil || s.DescribeTarget() == nil {
		return nil, nil, fmt.Errorf("failed to parse USE statement")
	}

	d := s.DescribeTarget()

	var result types.IPreparedQuery
	if d.KwKeyspaces() != nil { // "DESC KEYSPACES;"
		result = types.NewDescribeKeyspacesQuery(query.RawCql())
	} else if d.KwTables() != nil { // "DESC TABLES;"
		result = types.NewDescribeTablesQuery(query.RawCql(), sessionKeyspace)
	} else if d.KwTable() != nil { // "DESC TABLE [$KEYSPACE.]$TABLE;"
		if d.Table() == nil {
			return nil, nil, errors.New("invalid describe command. table name required")
		}
		table := types.TableName(d.Table().GetText())
		keyspace := sessionKeyspace
		if d.Keyspace() != nil {
			keyspace = types.Keyspace(d.Keyspace().GetText())
		}
		result = types.NewDescribeTable(query.RawCql(), keyspace, table)
	} else if d.KwKeyspace() != nil { // "DESC KEYSPACE $KEYSPACE;"
		keyspace := sessionKeyspace
		if d.Keyspace() != nil {
			keyspace = types.Keyspace(d.Keyspace().GetText())
		}
		if keyspace == "" {
			return nil, nil, errors.New("expected keyspace name after DESCRIBE KEYSPACE")
		}
		result = types.NewDescribeTablesQuery(query.RawCql(), keyspace)
	} else {
		return nil, nil, errors.New("failed to parse describe query")
	}
	return result, types.EmptyQueryParameterValues(), nil
}

func (t *DescTranslator) Bind(st types.IPreparedQuery, _ *types.QueryParameterValues, _ primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	desc, ok := st.(*types.DescribeQuery)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	return desc, nil
}

func (t *DescTranslator) QueryType() types.QueryType {
	return types.QueryTypeDescribe
}

func NewDescTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &DescTranslator{schemaMappingConfig: schemaMappingConfig}
}
