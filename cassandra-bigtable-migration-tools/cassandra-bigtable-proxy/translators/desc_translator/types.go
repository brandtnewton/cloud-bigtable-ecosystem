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

func (t *DescTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, _ bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	s := query.Parser().DescribeStatement()
	if s == nil || s.DescribeTarget() == nil {
		return nil, nil, fmt.Errorf("failed to parse USE statement")
	}

	d := s.DescribeTarget()

	if d.KwKeyspaces() != nil { // "DESC KEYSPACES;"
		return nil, types.NewDescribeKeyspacesQuery(query.RawCql()), nil
	} else if d.KwTables() != nil { // "DESC TABLES;"
		return nil, types.NewDescribeTablesQuery(query.RawCql(), sessionKeyspace), nil
	} else if d.KwTable() != nil { // "DESC TABLE [$KEYSPACE.]$TABLE;"
		if d.Table() == nil {
			return nil, nil, errors.New("invalid describe command. table name required")
		}
		table := types.TableName(d.Table().GetText())
		keyspace := sessionKeyspace
		if d.Keyspace() != nil {
			keyspace = types.Keyspace(d.Keyspace().GetText())
		}
		return nil, types.NewDescribeTable(query.RawCql(), keyspace, table), nil
	} else if d.KwKeyspace() != nil { // "DESC KEYSPACE $KEYSPACE;"
		keyspace := sessionKeyspace
		if d.Keyspace() != nil {
			keyspace = types.Keyspace(d.Keyspace().GetText())
		}
		if keyspace == "" {
			return nil, nil, errors.New("expected keyspace name after DESCRIBE KEYSPACE")
		}
		return nil, types.NewDescribeTablesQuery(query.RawCql(), keyspace), nil
	}
	return nil, nil, errors.New("failed to parse describe query")
}

func (t *DescTranslator) Bind(st types.IPreparedQuery, values []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	panic("cannot bind DESCRIBE statement")
}

func (t *DescTranslator) QueryType() types.QueryType {
	return types.QueryTypeDescribe
}

func NewDescTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &DescTranslator{schemaMappingConfig: schemaMappingConfig}
}
