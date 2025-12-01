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
package metadata

import (
	"reflect"
	"testing"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/assert"
)

func getSchemaMappingConfig() *SchemaMetadata {
	return NewSchemaMetadata(
		"cf1",
		[]*TableSchema{
			NewTableConfig(
				"keyspace",
				"table1",
				"cf1",
				types.OrderedCodeEncoding,
				[]*types.Column{
					{
						Name:         "column1",
						CQLType:      types.TypeVarchar,
						KeyType:      types.KeyTypeRegular,
						ColumnFamily: "cf1",
					},
					{
						Name:         "column2",
						CQLType:      types.TypeInt,
						KeyType:      types.KeyTypeRegular,
						ColumnFamily: "cf1",
					},
				},
			),
			NewTableConfig(
				"keyspace",
				"table2",
				"cf1",
				types.OrderedCodeEncoding,
				[]*types.Column{
					{
						Name:         "id",
						CQLType:      types.TypeInt,
						KeyType:      "partition",
						IsPrimaryKey: true,
					},
					{
						Name:         "name",
						CQLType:      types.TypeVarchar,
						KeyType:      "clustering",
						IsPrimaryKey: true,
					},
				},
			),
		},
	)
}

var expectedResponse = []*message.ColumnMetadata{
	{Keyspace: "keyspace", Name: "column1", Table: "table1", Type: datatype.Varchar, Index: 0},
}

func Test_GetColumn(t *testing.T) {
	columnExistsInDifferentTableArgs := struct {
		tableName  string
		columnName string
	}{
		tableName:  "table1",
		columnName: "column3",
	}

	tests := []struct {
		name   string
		fields *SchemaMetadata
		args   struct {
			tableName  string
			columnName string
		}
		want    *types.Column
		wantErr bool
	}{
		{
			name:   "types.Columns exists",
			fields: getSchemaMappingConfig(),
			args: struct {
				tableName  string
				columnName string
			}{
				tableName:  "table1",
				columnName: "column1",
			},
			want: &types.Column{
				Name:         "column1",
				ColumnFamily: "cf1",
				IsPrimaryKey: false,
				CQLType:      types.TypeVarchar,
			},
			wantErr: false,
		},
		{
			name:    "types.Columns exists in different table",
			fields:  getSchemaMappingConfig(),
			args:    columnExistsInDifferentTableArgs,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc, err := tt.fields.GetTableConfig("keyspace", types.TableName(tt.args.tableName))
			if err != nil {
				t.Errorf("table config error: %v", err)
				return
			}
			got, err := tc.GetColumn(types.ColumnName(tt.args.columnName))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want.Name, got.Name)
				assert.Equal(t, tt.want.ColumnFamily, got.ColumnFamily)
				assert.Equal(t, tt.want.IsPrimaryKey, got.IsPrimaryKey)
				assert.Equal(t, tt.want.CQLType, got.CQLType)
			}
		})
	}
}

func Test_ListKeyspaces(t *testing.T) {
	tests := []struct {
		name     string
		tables   map[string]map[string]*TableSchema
		expected []string
	}{
		{
			name: "Multiple keyspaces, unsorted input",
			tables: map[string]map[string]*TableSchema{
				"zeta":  {},
				"alpha": {},
				"beta":  {},
			},
			expected: []string{"alpha", "beta", "zeta"},
		},
		{
			name: "Single keyspace",
			tables: map[string]map[string]*TableSchema{
				"only": {},
			},
			expected: []string{"only"},
		},
		{
			name:     "No keyspaces",
			tables:   map[string]map[string]*TableSchema{},
			expected: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SchemaMetadata{tables: tt.tables}
			got := cfg.ListKeyspaces()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func Test_sortPrimaryKeysData(t *testing.T) {
	type args struct {
		pkMetadata []*types.Column
	}
	tests := []struct {
		name string
		args args
		want []*types.Column
	}{
		{
			name: "Basic Sorting",
			args: args{
				pkMetadata: []*types.Column{
					{Name: "id", PkPrecedence: 2},
					{Name: "email", PkPrecedence: 1},
				},
			},
			want: []*types.Column{
				{Name: "email", PkPrecedence: 1},
				{Name: "id", PkPrecedence: 2},
			},
		},
		{
			name: "Already Sorted Data",
			args: args{
				pkMetadata: []*types.Column{
					{Name: "order_id", PkPrecedence: 1},
					{Name: "customer_id", PkPrecedence: 2},
				},
			},
			want: []*types.Column{
				{Name: "order_id", PkPrecedence: 1},
				{Name: "customer_id", PkPrecedence: 2},
			},
		},
		{
			name: "Same Precedence Columns (Unchanged Order)",
			args: args{
				pkMetadata: []*types.Column{
					{Name: "product_id", PkPrecedence: 1},
					{Name: "category_id", PkPrecedence: 1},
				},
			},
			want: []*types.Column{
				{Name: "product_id", PkPrecedence: 1},
				{Name: "category_id", PkPrecedence: 1}, // Order should remain the same
			},
		},
		{
			name: "Single types.Columns (No Sorting Needed)",
			args: args{
				pkMetadata: []*types.Column{
					{Name: "category_id", PkPrecedence: 1},
				},
			},
			want: []*types.Column{
				{Name: "category_id", PkPrecedence: 1},
			},
		},
		{
			name: "Empty Slice (No Operation)",
			args: args{
				pkMetadata: []*types.Column{},
			},
			want: []*types.Column{},
		},
		{
			name: "Table With Nil Columns (Should Not Panic)",
			args: args{
				pkMetadata: nil,
			},
			want: nil,
		},
		{
			name: "Negative Precedence Columns (Still Sorted Correctly)",
			args: args{
				pkMetadata: []*types.Column{
					{Name: "col1", PkPrecedence: -1},
					{Name: "col2", PkPrecedence: -3},
					{Name: "col3", PkPrecedence: -2},
				},
			},
			want: []*types.Column{
				{Name: "col2", PkPrecedence: -3},
				{Name: "col3", PkPrecedence: -2},
				{Name: "col1", PkPrecedence: -1},
			},
		},
		{
			name: "Zero Precedence Columns (Sorted Normally)",
			args: args{
				pkMetadata: []*types.Column{
					{Name: "colA", PkPrecedence: 0},
					{Name: "colB", PkPrecedence: 2},
					{Name: "colC", PkPrecedence: 1},
				},
			},
			want: []*types.Column{
				{Name: "colA", PkPrecedence: 0},
				{Name: "colC", PkPrecedence: 1},
				{Name: "colB", PkPrecedence: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sortPrimaryKeys(tt.args.pkMetadata)
			if !reflect.DeepEqual(tt.args.pkMetadata, tt.want) {
				t.Errorf("sortPrimaryKeys() = %v, want %v", tt.args.pkMetadata, tt.want)
			}
		})
	}
}
