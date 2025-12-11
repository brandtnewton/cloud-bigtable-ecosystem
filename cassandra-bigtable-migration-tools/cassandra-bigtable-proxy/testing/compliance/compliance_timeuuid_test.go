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
	require.NoError(t, session.Query(fmt.Sprintf(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time = %s`, t1.String())).Scan(&got.region, &got.eventTime, &got.measurement, &got.parentEvent))

	assert.Equal(t, "timeuuid-literal", got.region)
	assert.Equal(t, t1, got.eventTime)
	assert.Equal(t, int32(3), got.measurement)
	assert.Equal(t, t2, got.parentEvent)
}

func TestMaxAndMinTimestamp(t *testing.T) {
	t.Parallel()
	t1 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', ?, 3, now())`, t1).Exec())
	t2 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', ?, 3, now())`, t2).Exec())
	t3 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', ?, 3, now())`, t3).Exec())
	t4 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', ?, 3, now())`, t4).Exec())
	t5 := gocql.TimeUUID()
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.timeuuid_table (region, event_time, measurement, parent_event) VALUES ('timeuuid-literal', ?, 3, now())`, t5).Exec())

	var scanner gocql.Scanner
	var got []gocql.UUID
	var err error
	scanner = session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time < minTimeuuid(?)`, t5.Time()).Iter().Scanner()
	got, err = scanAllTimeuuids(scanner)
	require.NoError(t, err)
	assert.Equal(t, []gocql.UUID{t1, t2, t3, t4}, got)

	scanner = session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time > minTimeuuid(?)`, t5.Time()).Iter().Scanner()
	got, err = scanAllTimeuuids(scanner)
	require.NoError(t, err)
	assert.Equal(t, []gocql.UUID{t5}, got)

	scanner = session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time > maxTimeuuid(?)`, t5.Timestamp()).Iter().Scanner()
	got, err = scanAllTimeuuids(scanner)
	require.NoError(t, err)
	assert.Equal(t, nil, got)

	scanner = session.Query(`SELECT region, event_time, measurement, parent_event FROM bigtabledevinstance.timeuuid_table WHERE event_time < maxTimeuuid(?)`, t5).Iter().Scanner()
	got, err = scanAllTimeuuids(scanner)
	require.NoError(t, err)
	assert.Equal(t, []gocql.UUID{t1, t2, t3, t4, t5}, got)
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
