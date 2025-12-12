package compliance

import (
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
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', 6c9b87f6-d764-11f0-8f97-8e0ad7a51247, 3, 6c9b87f6-d764-11f0-8f98-8e0ad7a51247)`).Exec())
	got := TimeUuidEvent{}

	require.NoError(t, session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time = 6c9b87f6-d764-11f0-8f97-8e0ad7a51247`).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))

	assert.Equal(t, "timeuuid-literal", got.region)
	assert.Equal(t, "6c9b87f6-d764-11f0-8f97-8e0ad7a51247", got.eventTime.String())
	assert.Equal(t, int32(3), got.measurement)
	assert.Equal(t, "6c9b87f6-d764-11f0-8f98-8e0ad7a51247", got.parentEvent.String())
}

func TestValidateMaxAndMin(t *testing.T) {
	inputTime := time.Date(2025, 12, 12, 13, 20, 42, 456000000, time.UTC)

	minUuid, err := gocql.ParseUUID("5c09c580-d75d-11f0-8080-808080808080")
	require.NoError(t, err)
	maxUuid, err := gocql.ParseUUID("5c09ec8f-d75d-11f0-7f7f-7f7f7f7f7f7f")
	require.NoError(t, err)

	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-validate', ?, 3, now())`, minUuid).Exec())
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-validate', ?, 3, now())`, maxUuid).Exec())

	var gotMin gocql.UUID
	require.NoError(t, session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-validate' AND event_time = minTimeuuid(?)`, inputTime).Scan(&gotMin))
	assert.Equal(t, minUuid, gotMin)
	var gotMax gocql.UUID
	require.NoError(t, session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-validate' AND event_time = maxTimeuuid(?)`, inputTime).Scan(&gotMax))
	assert.Equal(t, maxUuid, gotMax)
}
func TestMaxAndMinTimestamp(t *testing.T) {
	t.Parallel()

	t1, _ := gocql.ParseUUID("7b287b9e-d769-11f0-b949-8e0ad7a51247")
	t2, _ := gocql.ParseUUID("7e2fcec8-d769-11f0-b94a-8e0ad7a51247")
	// 2025-12-12 14:47:38.769 +0000
	// server: 2025-12-12 14:47:38.769 +0000 OK
	// max: 8133f810-d769-11f0-ffff-ffffffffffff WHAT
	t3, _ := gocql.ParseUUID("8133fa68-d769-11f0-b94b-8e0ad7a51247")

	t4, _ := gocql.ParseUUID("843a7228-d769-11f0-b94c-8e0ad7a51247")
	t5, _ := gocql.ParseUUID("87575e30-d769-11f0-b94d-8e0ad7a51247")

	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t1).Exec())
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t2).Exec())
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t3).Exec())
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t4).Exec())
	require.NoError(t, session.Query(`INSERT INTO timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-minmax', ?, 3, now())`, t5).Exec())

	t.Run("< timeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time < ?`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t1, t2}, got)
	})

	t.Run("> timeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time > ?`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t4, t5}, got)
	})

	t.Run(">= timeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time >= ?`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t3, t4, t5}, got)
	})

	t.Run("<= timeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time <= ?`, t3).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t1, t2, t3}, got)
	})

	t.Run("< minTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time < minTimeuuid(?)`, t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.NotContains(t, got, t3)
		assert.Equal(t, []gocql.UUID{t1, t2}, got)
	})

	t.Run("> minTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time > minTimeuuid(?)`, t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t3, t4, t5}, got)
	})

	t.Run("> maxTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time > maxTimeuuid(?)`, t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.NotContains(t, got, t3)
		assert.Equal(t, []gocql.UUID{t4, t5}, got)
	})

	t.Run("< maxTimeuuid", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time < maxTimeuuid(?)`, t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.Contains(t, got, t1)
		assert.Contains(t, got, t2)
		assert.Contains(t, got, t3)
		assert.Equal(t, []gocql.UUID{t1, t2, t3}, got)
	})

	t.Run("between t1 t3", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time BETWEEN ? AND ?`, t1, t3).Iter().Scanner())
		require.NoError(t, err)
		// between is inclusive
		assert.Equal(t, []gocql.UUID{t1, t2, t3}, got)
	})

	t.Run("between min max", func(t *testing.T) {
		t.Parallel()
		got, err := scanAllTimeuuids(session.Query(`SELECT event_time FROM timeuuid_table WHERE region='timeuuid-minmax' AND event_time BETWEEN  minTimeuuid(?) AND maxTimeuuid(?)`, t3.Time(), t3.Time()).Iter().Scanner())
		require.NoError(t, err)
		assert.Equal(t, []gocql.UUID{t3}, got)
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
