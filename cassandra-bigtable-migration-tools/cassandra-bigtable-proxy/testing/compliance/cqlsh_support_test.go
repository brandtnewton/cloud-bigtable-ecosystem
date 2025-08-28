package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCqlshCrud(t *testing.T) {
	t.Parallel()

	_, err := executeCQLQuery(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES ('cqlsh_person', 80, 25)`)
	require.NoError(t, err, "Failed to insert record")

	results, err := executeCQLQuery(`SELECT name, age, code FROM bigtabledevinstance.user_info where name='cqlsh_person' and age=80`)
	require.NoError(t, err, "Failed to insert record")
	assert.Equal(t, []map[string]string{{"age": "80", "name": "cqlsh_person", "code": "25"}}, results)
}
