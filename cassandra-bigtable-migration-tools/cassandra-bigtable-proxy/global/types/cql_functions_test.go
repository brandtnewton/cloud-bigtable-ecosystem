package types

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMaxUUIDv1ForTime(t *testing.T) {
	date := time.Date(2025, 12, 11, 5, 45, 11, 300, time.UTC)
	got, err := maxUUIDv1ForTime(date)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(0x1), got.Version())
	gotT, err := getTimeFromUUID(got)
	require.NoError(t, err)
	assert.Equal(t, date.Truncate(time.Millisecond), gotT.Truncate(time.Millisecond))
}

func TestMinUUIDv1ForTime(t *testing.T) {
	date := time.Date(2025, 12, 11, 5, 45, 11, 300, time.UTC)
	got, err := minUUIDv1ForTime(date)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(0x1), got.Version())
	gotT, err := getTimeFromUUID(got)
	require.NoError(t, err)
	assert.Equal(t, date, gotT)
}

func TestMinMaxUUIDForDate(t *testing.T) {
	date := time.Date(2025, 12, 11, 5, 45, 11, 300, time.UTC)

	minUuid, err := minUUIDv1ForTime(date)
	require.NoError(t, err)

	maxUuid, err := maxUUIDv1ForTime(date)
	require.NoError(t, err)

	assert.NotEqual(t, minUuid.String(), maxUuid.String())
	assert.Less(t, minUuid.String(), maxUuid.String())

	// 101 because 100 nanos is enough to be the same uuid
	minUuidPlus, err := minUUIDv1ForTime(date.Add(101 * time.Nanosecond))
	require.NoError(t, err)
	assert.Less(t, minUuid.String(), minUuidPlus.String())
	assert.Less(t, maxUuid.String(), minUuidPlus.String())

	maxUuidPlus, err := maxUUIDv1ForTime(date.Add(101 * time.Nanosecond))
	require.NoError(t, err)
	assert.Less(t, maxUuid.String(), maxUuidPlus.String())
}
func TestMinMaxUUIDForUUID(t *testing.T) {
	u, err := uuid.NewUUID()
	require.NoError(t, err)

	date, err := getTimeFromUUID(u)
	require.NoError(t, err)

	minUuid, err := minUUIDv1ForTime(date)
	require.NoError(t, err)

	maxUuid, err := maxUUIDv1ForTime(date)
	require.NoError(t, err)

	assert.Less(t, u.String(), maxUuid.String())
	assert.Less(t, minUuid.String(), u.String())
}

// these values are taken directly from cassandra for the given date
func TestMaxTimeUUID(t *testing.T) {
	inputTime := time.Date(2025, 12, 12, 13, 20, 42, 456000000, time.UTC)
	//inputTimeString := "2025-12-12 13:20:42.456"

	minUuid, err := minUUIDv1ForTime(inputTime)
	require.NoError(t, err)
	assert.Equal(t, "5c09c580-d75d-11f0-8080-808080808080", minUuid.String())
	minUuidTime, err := getTimeFromUUID(minUuid)
	require.NoError(t, err)
	assert.Equal(t, inputTime, minUuidTime.Truncate(time.Millisecond))

	maxUuid, err := maxUUIDv1ForTime(inputTime)
	require.NoError(t, err)
	assert.Equal(t, "5c09ec8f-d75d-11f0-7f7f-7f7f7f7f7f7f", maxUuid.String())
	maxUuidTime, err := getTimeFromUUID(maxUuid)
	require.NoError(t, err)
	assert.Equal(t, inputTime, maxUuidTime.Truncate(time.Millisecond))
}

func TestSetUuidV1Time(t *testing.T) {
	u, err := uuid.Parse("8133fa68-d769-11f0-b94b-8e0ad7a51247")
	require.NoError(t, err)
	uuidTime := time.Date(2025, 12, 12, 14, 47, 38, 769060000, time.UTC)

	b, err := u.MarshalBinary()
	require.NoError(t, err)
	u = setUuidV1Time(uuidTime, [16]byte(b))

	got, err := getTimeFromUUID(u)
	require.NoError(t, err)

	assert.Equal(t, uuidTime, got)

	gotMax, err := maxUUIDv1ForTime(uuidTime)
	require.NoError(t, err)
	gotMaxTime, err := getTimeFromUUID(gotMax)
	require.NoError(t, err)
	assert.Equal(t, uuidTime, gotMaxTime)

	gotMin, err := maxUUIDv1ForTime(uuidTime)
	require.NoError(t, err)
	gotMinTime, err := getTimeFromUUID(gotMin)
	require.NoError(t, err)
	assert.Equal(t, uuidTime, gotMinTime)
}
