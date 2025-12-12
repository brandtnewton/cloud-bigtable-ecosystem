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
	assert.Equal(t, date, gotT)
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
