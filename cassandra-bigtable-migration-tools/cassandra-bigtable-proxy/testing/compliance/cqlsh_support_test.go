package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCqlshCrud(t *testing.T) {
	t.Parallel()

	_, err := scanCQLSHQuery(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES ('cqlsh_person', 80, 25)`)
	require.NoError(t, err)

	results, err := scanCQLSHQuery(`SELECT name, age, code FROM bigtabledevinstance.user_info where name='cqlsh_person' and age=80`)
	require.NoError(t, err)
	assert.Equal(t, []map[string]string{{"age": "80", "name": "cqlsh_person", "code": "25"}}, results)
}

func TestCqlshDesc(t *testing.T) {
	t.Parallel()

	got, err := runCQLSHDescribe(`desc keyspaces`)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"bigtabledevinstance", "cassandrakeyspace"}, got)
}
