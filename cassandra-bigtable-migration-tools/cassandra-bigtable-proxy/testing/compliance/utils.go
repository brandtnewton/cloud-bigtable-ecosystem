/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper to parse RFC3339 timestamp strings used in the tests
func parseTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsedTime, err := time.Parse(time.RFC3339, ts)
	require.NoError(t, err, "Failed to parse timestamp string")
	return parsedTime
}

// Helper to parse "YYYY-MM-DD HH:MM:SS" timestamp strings used in the tests.
func parseSimpleTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsedTime, err := time.Parse("2006-01-02 15:04:05", ts)
	require.NoError(t, err, "Failed to parse timestamp string")
	return parsedTime
}
