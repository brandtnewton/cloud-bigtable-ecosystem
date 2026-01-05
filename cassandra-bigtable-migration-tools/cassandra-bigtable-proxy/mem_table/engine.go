package mem_table

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	"reflect"
	"slices"
	"strings"
	"sync"
)

type database map[types.Keyspace]map[types.TableName]*tableStore

type InMemEngine struct {
	tables database
	mu     sync.RWMutex
}

func NewInMemEngine() *InMemEngine {
	return &InMemEngine{
		tables: make(database),
	}
}

func (e *InMemEngine) GetTable(keyspace types.Keyspace, table types.TableName) ([]types.GoRow, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	k, ok := e.tables[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace '%s' not found", keyspace)
	}
	t, ok := k[table]
	if !ok {
		return nil, fmt.Errorf("table '%s' not found in keyspace '%s'", table, keyspace)
	}
	return t.Data, nil
}

func (e *InMemEngine) SetData(table *schemaMapping.TableSchema, data []types.GoRow) error {
	err := validateData(table, data)
	if err != nil {
		return err
	}

	// sort by primary keys
	slices.SortFunc(data, func(a, b types.GoRow) int {
		aKey := getKey(table, a)
		bKey := getKey(table, b)
		return strings.Compare(aKey, bKey)
	})

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tables[table.Keyspace]; !exists {
		e.tables[table.Keyspace] = make(map[types.TableName]*tableStore)
	}
	e.tables[table.Keyspace][table.Name] = newTableStore(table, data)
	return nil
}

func getKey(table *schemaMapping.TableSchema, data types.GoRow) string {
	var values []string
	for _, pmk := range table.PrimaryKeys {
		values = append(values, fmt.Sprint(data[string(pmk.Name)]))
	}
	return strings.Join(values, "#")
}

func validateData(table *schemaMapping.TableSchema, data []types.GoRow) error {
	for i, row := range data {
		for _, col := range table.Columns {
			value, ok := row[string(col.Name)]
			if !ok {
				return fmt.Errorf("missing data for column '%s' in row %d of table %s.%s", col.Name, i, table.Keyspace, table.Name)
			}
			if reflect.TypeOf(value) != col.CQLType.GoType() {
				return fmt.Errorf("invalid type column '%s' in row %d of table %s.%s: expected %s but got %T", col.Name, i, table.Keyspace, table.Name, col.CQLType.GoType().String(), value)
			}
		}
	}
	return nil
}
