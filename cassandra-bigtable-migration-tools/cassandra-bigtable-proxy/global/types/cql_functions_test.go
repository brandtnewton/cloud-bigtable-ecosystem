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
