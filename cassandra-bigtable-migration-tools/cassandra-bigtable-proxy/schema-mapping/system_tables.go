package schemaMapping

import "github.com/google/uuid"

func ReadSystemColumns(config *SchemaMappingConfig) []map[string]interface{} {
	var results []map[string]interface{} = nil
	for _, tables := range config.GetAllTables() {
		for _, tableConfig := range tables {
			for _, column := range tableConfig.Columns {
				row := map[string]interface{}{
					"keyspace_name":    tableConfig.Keyspace,
					"table_name":       tableConfig.Name,
					"column_name":      column.Name,
					"clustering_order": "none",
					// todo make sure this is the correct bytes representation
					"column_name_bytes": []byte(column.Name),
					"kind":              column.KeyType,
					"position":          column.PkPrecedence,
					"type":              column.CQLType.String(),
				}
				results = append(results, row)
			}
		}
	}
	return results
}

func ReadSystemTable(config *SchemaMappingConfig) []map[string]interface{} {
	var results []map[string]interface{} = nil
	for _, tables := range config.GetAllTables() {
		for _, tableConfig := range tables {
			row := map[string]interface{}{
				"keyspace_name":           tableConfig.Keyspace,
				"table_name":              tableConfig.Name,
				"additional_write_policy": "99p",
				"bloom_filter_fp_chance":  ".01",
				"caching": map[string]string{
					"keys": "ALL", "rows_per_partition": "NONE",
				},
				"cdc":     nil,
				"comment": "",
				"compaction": map[string]string{
					"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold": "32", "min_threshold": "4",
				},
				"compression": map[string]string{
					"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor",
				},
				"crc_check_chance":            1,
				"dclocal_read_repair_chance":  0,
				"default_time_to_live":        0,
				"extensions":                  map[string]string{},
				"flags":                       []string{"compound"},
				"gc_grace_seconds":            864000,
				"id":                          uuid.New().String(),
				"max_index_interval":          2048,
				"memtable":                    nil,
				"memtable_flush_period_in_ms": 0,
				"min_index_interval":          128,
				"read_repair":                 "BLOCKING",
				"read_repair_chance":          0,
				"speculative_retry":           "99p",
			}
			results = append(results, row)
		}
	}

	return results
}
