package compliance

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// todo write literal uuid
// todo where maxTimeuuid
// todo where minTimeuuid
// todo select time from timeuuid
func TestInsertTimeUuidNow(t *testing.T) {
	t.Parallel()

	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-now', now(), 3, now())`).Exec())
	got := TimeUuidEvent{}
	require.NoError(t, session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE region = 'timeuuid-now'`).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))

	assert.Equal(t, "timeuuid-now", got.region)
	assert.Equal(t, int32(3), got.measurement)
}

func TestInsertTimeUuidLiteral(t *testing.T) {
	t.Parallel()
	t1 := gocql.TimeUUID()
	t2 := gocql.TimeUUID()
	require.NoError(t, session.Query(fmt.Sprintf(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', %s, 3, %s)`, t1.String(), t2.String())).Exec())
	got := TimeUuidEvent{}
	require.NoError(t, session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE region = 'timeuuid-literal'`).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))

	assert.Equal(t, "timeuuid-literal", got.region)
	assert.Equal(t, t1, got.eventTime)
	assert.Equal(t, int32(3), got.measurement)
	assert.Equal(t, t2, got.parentEvent)
}

func TestInsertTimeUuidPrepared(t *testing.T) {
	t.Parallel()

	t1 := gocql.TimeUUID()
	t2 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-prepared', ?, 3, ?)`, t1, t2).Exec())
	got := TimeUuidEvent{}
	require.NoError(t, session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE region = 'timeuuid-prepared'`).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))
	assert.Equal(t, "timeuuid-prepared", got.region)
	assert.Equal(t, t1, got.eventTime)
	assert.Equal(t, int32(3), got.measurement)
	assert.Equal(t, t2, got.parentEvent)

	require.NoError(t, session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time=? AND parent_event=?`, t1, t2).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))
	assert.Equal(t, "timeuuid-prepared", got.region)
	assert.Equal(t, t1, got.eventTime)
	assert.Equal(t, int32(3), got.measurement)
	assert.Equal(t, t2, got.parentEvent)
}

type TimeUuidEvent struct {
	region      string
	eventTime   gocql.UUID
	measurement int32
	parentEvent gocql.UUID
}
