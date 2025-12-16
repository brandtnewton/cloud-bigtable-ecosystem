package use_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTranslator_TranslateUpdateQuerytoBigtable(t *testing.T) {
	type Want struct {
		Keyspace types.Keyspace
	}

	tests := []struct {
		name            string
		sessionKeyspace types.Keyspace
		query           string
		want            *Want
		wantErr         string
	}{
		{
			name:  "success",
			query: "USE test_keyspace",
			want: &Want{
				Keyspace: "test_keyspace",
			},
		},
		{
			name:    "unknown",
			query:   "USE unknown_keyspace",
			wantErr: "keyspace 'unknown_keyspace' does not exist",
		},
		{
			name:    "update boolean column",
			query:   "use keyspace test_keyspace",
			wantErr: "parsing error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewUseTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.sessionKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeUse), tt.sessionKeyspace)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			gotUpdate := got.(*types.UseTableStatementMap)
			assert.Equal(t, tt.want.Keyspace, gotUpdate.Keyspace())
		})
	}
}
