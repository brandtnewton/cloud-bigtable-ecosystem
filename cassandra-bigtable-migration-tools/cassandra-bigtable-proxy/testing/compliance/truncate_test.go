package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensures that various int row keys are handled correctly in all CRUD operations
func TestTruncate(t *testing.T) {
	t.Parallel()

	require.NoError(t, session.Query("CREATE TABLE IF NOT EXISTS truncate_test (id INT PRIMARY KEY, name TEXT").Exec())

	// add some data to truncate
	require.NoError(t, session.Query("INSERT INTO truncate_test (id, name) VALUES (?, ?)", 1, "foo").Exec())
	require.NoError(t, session.Query("INSERT INTO truncate_test (id, name) VALUES (?, ?)", 2, "bar").Exec())

	var gotCount int32
	query := session.Query("select count(*) from truncate_test")
	require.NoError(t, query.Scan(&gotCount))

	// ensure the data is there
	assert.Equal(t, 2, gotCount)

	require.NoError(t, session.Query("TRUNCATE TABLE truncate_test").Exec())

	require.NoError(t, query.Scan(&gotCount))

	// ensure the data is gone
	assert.Equal(t, 0, gotCount)
}
