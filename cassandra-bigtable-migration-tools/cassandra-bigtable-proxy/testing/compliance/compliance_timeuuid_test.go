package compliance

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
	require.NoError(t, session.Query(fmt.Sprintf(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time = %s`, t1.String())).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))

	assert.Equal(t, "timeuuid-literal", got.region)
	assert.Equal(t, t1, got.eventTime)
	assert.Equal(t, int32(3), got.measurement)
	assert.Equal(t, t2, got.parentEvent)
}

func TestMaxAndMinTimestamp(t *testing.T) {
	t.Parallel()
	t1 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t1).Exec())
	time.Sleep(500 * time.Millisecond)
	t2 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t2).Exec())
	time.Sleep(500 * time.Millisecond)
	t3 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t3).Exec())
	time.Sleep(500 * time.Millisecond)
	t4 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t4).Exec())
	time.Sleep(500 * time.Millisecond)
	t5 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t5).Exec())

	t.Run("< timeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' event_time < ?`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t1, t2}, got)
	})

	t.Run("<= timeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' event_time <= ?`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t1, t2, t3}, got)
	})

	t.Run("< minTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' event_time < minTimeuuid(?)`, t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t1, t2}, got)
	})

	t.Run("> minTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' event_time > minTimeuuid(?)`, t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t3, t4, t5}, got)
	})

	t.Run("> maxTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' event_time > maxTimeuuid(?)`, t3.Timestamp()).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t4, t5}, got)
	})

	t.Run("< minTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' event_time < maxTimeuuid(?)`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t1, t2, t3}, got)
	})
}

func scanAllTimeuuids(scanner gocql.Scanner) ([]gocql.UUID, error) {
	var results []gocql.UUID = nil
	for scanner.Next() {
		var t gocql.UUID
		err := scanner.Scan(
			&t,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return results, nil
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
