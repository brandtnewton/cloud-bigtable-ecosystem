package compliance

import (
	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// DO NOT CHANGE VALUES - these values are hardcoded to ensure we keep backwards compatibility between versions
func TestBackwardsCompatibleData(t *testing.T) {
	t.Parallel()
	if testTarget == TestTargetCassandra {
		t.Skip()
		return
	}

	testCases := []struct {
		name             string
		query            string
		rowKey           string
		ignoreColumnName bool
		want             bigtable.Row
	}{
		{
			name:   "boolean false",
			query:  `INSERT INTO all_columns (name, bool_col) VALUES ('compatability_1', false) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_1",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_1",
						Column:    "cf1:bool_col",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
						Labels:    nil,
					},
				},
			},
		},
		{
			name:   "boolean true",
			query:  `INSERT INTO all_columns (name, bool_col) VALUES ('compatability_2', true) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_2",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_2",
						Column:    "cf1:bool_col",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
						Labels:    nil,
					},
				},
			},
		},
		// --- PRIMITIVES ---
		{
			name:   "int column",
			query:  `INSERT INTO all_columns (name, int_col) VALUES ('compatability_3', 100) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_3",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_3",
						Column:    "cf1:int_col",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64},
						Labels:    nil,
					},
				},
			},
		},
		{
			name:   "bigint column",
			query:  `INSERT INTO all_columns (name, bigint_col) VALUES ('compatability_4', 100) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_4",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_4",
						Column:    "cf1:bigint_col",
						Timestamp: 1764084624204000,
						// 100 in 8-byte Big Endian
						Value:  []uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64},
						Labels: nil,
					},
				},
			},
		},
		{
			name:   "text column",
			query:  `INSERT INTO all_columns (name, text_col) VALUES ('compatability_5', 'hello') USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_5",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_5",
						Column:    "cf1:text_col",
						Timestamp: 1764084624204000,
						Value:     []byte("hello"),
						Labels:    nil,
					},
				},
			},
		},
		{
			name:   "float column",
			query:  `INSERT INTO all_columns (name, float_col) VALUES ('compatability_6', 1.5) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_6",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_6",
						Column:    "cf1:float_col",
						Timestamp: 1764084624204000,
						// 1.5 in IEEE 754 4-byte Big Endian
						Value:  []uint8{0x3F, 0xC0, 0x00, 0x00},
						Labels: nil,
					},
				},
			},
		},
		{
			name:   "double column",
			query:  `INSERT INTO all_columns (name, double_col) VALUES ('compatability_7', 1.5) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_7",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_7",
						Column:    "cf1:double_col",
						Timestamp: 1764084624204000,
						// 1.5 in IEEE 754 8-byte Big Endian
						Value:  []uint8{0x3F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
						Labels: nil,
					},
				},
			},
		},
		{
			name:   "timestamp column",
			query:  `INSERT INTO all_columns (name, time_col) VALUES ('compatability_8', '2024-08-12T12:34:56Z') USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_8",
			want: bigtable.Row{
				"cf1": []bigtable.ReadItem{
					{
						Row:       "compatability_8",
						Column:    "cf1:time_col",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x0, 0x0, 0x1, 0x91, 0x46, 0x95, 0x9d, 0x80},
						Labels:    nil,
					},
				},
			},
		},

		// --- MAPS ---
		// Note: Replace []byte("TODO...") with the actual serialized bytes your system produces for maps.
		{
			name:   "map<text, text>",
			query:  `INSERT INTO all_columns (name, map_text_text) VALUES ('compatability_9', {'key1': 'val1'}) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_9",
			want: bigtable.Row{
				"map_text_text": []bigtable.ReadItem{
					{
						Row:       "compatability_9",
						Column:    "map_text_text:key1",
						Timestamp: 1764084624204000,
						Value:     []byte("val1"),
						Labels:    nil,
					},
				},
			},
		},
		{
			name:   "map<text, int>",
			query:  `INSERT INTO all_columns (name, map_text_int) VALUES ('compatability_10', {'stat1': 500}) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_10",
			want: bigtable.Row{
				"map_text_int": []bigtable.ReadItem{
					{
						Row:       "compatability_10",
						Column:    "map_text_int:stat1",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xf4},
						Labels:    nil,
					},
				},
			},
		},
		{
			name:   "map<timestamp, double>",
			query:  `INSERT INTO all_columns (name, ts_double_map) VALUES ('compatability_11', {'2024-01-02T12:34:56Z': 99.9}) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_11",
			want: bigtable.Row{
				"ts_double_map": []bigtable.ReadItem{
					{
						Row:       "compatability_11",
						Column:    "ts_double_map:2024-01-02T12:34:56Z",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x40, 0x58, 0xf9, 0x99, 0x99, 0x99, 0x99, 0x9a},
						Labels:    nil,
					},
				},
			},
		},

		// --- SETS ---
		{
			name:   "set<text>",
			query:  `INSERT INTO all_columns (name, set_text) VALUES ('compatability_12', {'tag1', 'tag2'}) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_12",
			want: bigtable.Row{
				"set_text": []bigtable.ReadItem{
					{
						Row:       "compatability_12",
						Column:    "set_text:tag1",
						Timestamp: 1764084624204000,
						Value:     nil,
						Labels:    nil,
					},
					{
						Row:       "compatability_12",
						Column:    "set_text:tag2",
						Timestamp: 1764084624204000,
						Value:     nil,
						Labels:    nil,
					},
				},
			},
		},
		{
			name:   "set<int>",
			query:  `INSERT INTO all_columns (name, set_int) VALUES ('compatability_13', {1, 2, 3}) USING TIMESTAMP 1764084624204000`,
			rowKey: "compatability_13",
			want: bigtable.Row{
				"set_int": []bigtable.ReadItem{
					{
						Row:       "compatability_13",
						Column:    "set_int:1",
						Timestamp: 1764084624204000,
						Value:     nil,
						Labels:    nil,
					},
					{
						Row:       "compatability_13",
						Column:    "set_int:2",
						Timestamp: 1764084624204000,
						Value:     nil,
						Labels:    nil,
					},
					{
						Row:       "compatability_13",
						Column:    "set_int:3",
						Timestamp: 1764084624204000,
						Value:     nil,
						Labels:    nil,
					},
				},
			},
		},

		// --- LISTS ---
		{
			name:             "list<text>",
			query:            `INSERT INTO all_columns (name, list_text) VALUES ('compatability_14', ['item1', 'item2']) USING TIMESTAMP 1764084624204000`,
			rowKey:           "compatability_14",
			ignoreColumnName: true,
			want: bigtable.Row{
				"list_text": []bigtable.ReadItem{
					{
						Row:       "compatability_14",
						Column:    "ignored",
						Timestamp: 1764084624204000,
						Value:     []byte("item1"),
						Labels:    nil,
					},
					{
						Row:       "compatability_14",
						Column:    "ignored",
						Timestamp: 1764084624204000,
						Value:     []byte("item2"),
						Labels:    nil,
					},
				},
			},
		},
		{
			name:             "list<double>",
			query:            `INSERT INTO all_columns (name, list_double) VALUES ('compatability_15', [1.1, 2.2]) USING TIMESTAMP 1764084624204000`,
			rowKey:           "compatability_15",
			ignoreColumnName: true,
			want: bigtable.Row{
				"list_double": []bigtable.ReadItem{
					{
						Row:       "compatability_15",
						Column:    "ignored",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x3f, 0xf1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a},
						Labels:    nil,
					},
					{
						Row:       "compatability_15",
						Column:    "ignored",
						Timestamp: 1764084624204000,
						Value:     []uint8{0x40, 0x1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a},
						Labels:    nil,
					},
				},
			},
		},
	}

	table := client.Open("all_columns")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := cqlshExecWithKeyspace("bigtabledevinstance", tc.query)
			require.NoError(t, err)
			row, err := table.ReadRow(t.Context(), tc.rowKey, bigtable.RowFilter(bigtable.LatestNFilter(1)))
			require.NoError(t, err)

			if tc.ignoreColumnName {
				for _, cells := range row {
					for i := range cells {
						cv := cells[i]
						cv.Column = "ignored"
						cells[i] = cv
					}
				}
			}

			assert.Equal(t, tc.want, row)
		})
	}
}
