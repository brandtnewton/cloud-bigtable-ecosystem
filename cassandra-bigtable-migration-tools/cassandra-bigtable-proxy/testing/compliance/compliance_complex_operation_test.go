package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComplexOperationMapTextText(t *testing.T) {
	pkName, pkAge := "Walker White", int64(25)

	// 1. Insert and Validate
	initialMap := map[string]string{"key1": "value1", "key2": "value2"}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, extra_info) VALUES (?, ?, ?);`, pkName, pkAge, initialMap).Exec())
	var extraInfo map[string]string
	require.NoError(t, session.Query(`SELECT extra_info FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&extraInfo))
	assert.Equal(t, initialMap, extraInfo)

	// 2. Update (overwrite) and Validate
	overwriteMap := map[string]string{"key1": "value3", "key2": "value4"}
	require.NoError(t, session.Query(`UPDATE user_info SET extra_info = ? WHERE name = ? AND age = ?`, overwriteMap, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT extra_info FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&extraInfo))
	assert.Equal(t, overwriteMap, extraInfo)

	// 3. Add (merge) and Validate
	addMap := map[string]string{"key3": "value5", "key4": "value6"}
	require.NoError(t, session.Query(`UPDATE user_info SET extra_info = extra_info + ? WHERE name = ? AND age = ?`, addMap, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT extra_info FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&extraInfo))
	assert.Equal(t, map[string]string{"key1": "value3", "key2": "value4", "key3": "value5", "key4": "value6"}, extraInfo)

	// 4. Subtract (remove key) and Validate
	require.NoError(t, session.Query(`UPDATE user_info SET extra_info = extra_info - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT extra_info FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&extraInfo))
	assert.Equal(t, map[string]string{"key1": "value3", "key2": "value4", "key4": "value6"}, extraInfo)

	// 5. Update by key and Validate
	require.NoError(t, session.Query(`UPDATE user_info SET extra_info['key1'] = ? WHERE name = ? AND age = ?`, "value7", pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT extra_info FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&extraInfo))
	assert.Equal(t, map[string]string{"key1": "value7", "key2": "value4", "key4": "value6"}, extraInfo)

	// 6. Add by key and Validate
	require.NoError(t, session.Query(`UPDATE user_info SET extra_info['key5'] = ? WHERE name = ? AND age = ?`, "value8", pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT extra_info FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&extraInfo))
	assert.Equal(t, map[string]string{"key1": "value7", "key2": "value4", "key4": "value6", "key5": "value8"}, extraInfo)
}

func TestComplexOperationMapTextInt(t *testing.T) {
	pkName, pkAge := "Bobby Brown", int64(25)
	var currentMap map[string]int

	initialMap := map[string]int{"key1": 1, "key2": 2}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, map_text_int) VALUES (?, ?, ?);`, pkName, pkAge, initialMap).Exec())
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&currentMap))
	assert.Equal(t, initialMap, currentMap)

	overwriteMap := map[string]int{"key1": 3, "key2": 4}
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_int = ? WHERE name = ? AND age = ?`, overwriteMap, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&currentMap))
	assert.Equal(t, overwriteMap, currentMap)

	addMap := map[string]int{"key3": 5, "key4": 6}
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_int = map_text_int + ? WHERE name = ? AND age = ?`, addMap, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&currentMap))
	assert.Equal(t, map[string]int{"key1": 3, "key2": 4, "key3": 5, "key4": 6}, currentMap)

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_int = map_text_int - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&currentMap))
	assert.Equal(t, map[string]int{"key1": 3, "key2": 4, "key4": 6}, currentMap)

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_int['key1'] = ? WHERE name = ? AND age = ?`, 10, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&currentMap))
	assert.Equal(t, map[string]int{"key1": 10, "key2": 4, "key4": 6}, currentMap)

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_int['key5'] = ? WHERE name = ? AND age = ?`, 5, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&currentMap))
	assert.Equal(t, map[string]int{"key1": 10, "key2": 4, "key4": 6, "key5": 5}, currentMap)

	var finalMap map[string]int
	require.NoError(t, session.Query(`SELECT map_text_int FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&finalMap))
	assert.Equal(t, map[string]int{"key1": 10, "key2": 4, "key4": 6, "key5": 5}, finalMap)
}

func TestComplexOperationMapTextBigInt(t *testing.T) {
	pkName, pkAge := "Charlie Davis", int64(30)

	initialMap := map[string]int64{"key1": 1000000000, "key2": 2000000000}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, map_text_bigint) VALUES (?, ?, ?);`, pkName, pkAge, initialMap).Exec())

	overwriteMap := map[string]int64{"key1": 3000000000, "key2": 4000000000}
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_bigint = ? WHERE name = ? AND age = ?`, overwriteMap, pkName, pkAge).Exec())

	addMap := map[string]int64{"key3": 5000000000, "key4": 6000000000}
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_bigint = map_text_bigint + ? WHERE name = ? AND age = ?`, addMap, pkName, pkAge).Exec())

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_bigint = map_text_bigint - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkName, pkAge).Exec())

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_bigint['key1'] = ? WHERE name = ? AND age = ?`, int64(7000000000), pkName, pkAge).Exec())

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_bigint['key5'] = ? WHERE name = ? AND age = ?`, int64(8000000000), pkName, pkAge).Exec())

	var finalMap map[string]int64
	require.NoError(t, session.Query(`SELECT map_text_bigint FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&finalMap))
	assert.Equal(t, map[string]int64{"key1": 7000000000, "key2": 4000000000, "key4": 6000000000, "key5": 8000000000}, finalMap)
}

func TestComplexOperationMapTextBooleanAndFloat(t *testing.T) {
	// Boolean Map Test
	pkNameBool, pkAgeBool := "Diana Evans", int64(35)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, map_text_boolean) VALUES (?, ?, ?);`, pkNameBool, pkAgeBool, map[string]bool{"key1": true, "key2": false}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_boolean = ? WHERE name = ? AND age = ?`, map[string]bool{"key1": false, "key2": true}, pkNameBool, pkAgeBool).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_boolean = map_text_boolean + ? WHERE name = ? AND age = ?`, map[string]bool{"key3": true, "key4": false}, pkNameBool, pkAgeBool).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_boolean = map_text_boolean - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkNameBool, pkAgeBool).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_boolean['key1'] = ? WHERE name = ? AND age = ?`, true, pkNameBool, pkAgeBool).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_boolean['key5'] = ? WHERE name = ? AND age = ?`, false, pkNameBool, pkAgeBool).Exec())
	var finalMapBool map[string]bool
	require.NoError(t, session.Query(`SELECT map_text_boolean FROM user_info WHERE name = ? AND age = ?`, pkNameBool, pkAgeBool).Scan(&finalMapBool))
	assert.Equal(t, map[string]bool{"key1": true, "key2": true, "key4": false, "key5": false}, finalMapBool)

	// Float Map Test
	pkNameFloat, pkAgeFloat := "Eric Franklin", int64(40)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, map_text_float) VALUES (?, ?, ?);`, pkNameFloat, pkAgeFloat, map[string]float32{"key1": 1.5, "key2": 2.75}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_float = ? WHERE name = ? AND age = ?`, map[string]float32{"key1": 3.25, "key2": 4.5}, pkNameFloat, pkAgeFloat).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_float = map_text_float + ? WHERE name = ? AND age = ?`, map[string]float32{"key3": 5.75, "key4": 6.25}, pkNameFloat, pkAgeFloat).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_float = map_text_float - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkNameFloat, pkAgeFloat).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_float['key1'] = ? WHERE name = ? AND age = ?`, float32(10.0), pkNameFloat, pkAgeFloat).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_float['key5'] = ? WHERE name = ? AND age = ?`, float32(1.0), pkNameFloat, pkAgeFloat).Exec())
	var finalMapFloat map[string]float32
	require.NoError(t, session.Query(`SELECT map_text_float FROM user_info WHERE name = ? AND age = ?`, pkNameFloat, pkAgeFloat).Scan(&finalMapFloat))
	assert.Equal(t, map[string]float32{"key1": 10.0, "key2": 4.5, "key4": 6.25, "key5": 1.0}, finalMapFloat)
}

func TestComplexOperationMapTextDouble(t *testing.T) {
	pkName, pkAge := "Grace Harrison", int64(45)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, map_text_double) VALUES (?, ?, ?);`, pkName, pkAge, map[string]float64{"key1": 1.123, "key2": 2.987}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_double = ? WHERE name = ? AND age = ?`, map[string]float64{"key1": 3.141, "key2": 4.271}, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_double = map_text_double + ? WHERE name = ? AND age = ?`, map[string]float64{"key3": 5.555, "key4": 6.666}, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_double = map_text_double - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_double['key1'] = ? WHERE name = ? AND age = ?`, 10.0, pkName, pkAge).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_double['key5'] = ? WHERE name = ? AND age = ?`, 1.0, pkName, pkAge).Exec())

	var finalMap map[string]float64
	require.NoError(t, session.Query(`SELECT map_text_double FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&finalMap))
	assert.Equal(t, map[string]float64{"key1": 10.0, "key2": 4.271, "key4": 6.666, "key5": 1.0}, finalMap)
}

func TestComplexOperationMapTextTimestamp(t *testing.T) {
	pkName, pkAge := "Ivy Johnson", int64(50)

	initialMap := map[string]time.Time{"key1": parseSimpleTime(t, "2023-01-01 12:00:00"), "key2": parseSimpleTime(t, "2023-01-02 13:00:00")}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, map_text_ts) VALUES (?, ?, ?);`, pkName, pkAge, initialMap).Exec())

	overwriteMap := map[string]time.Time{"key3": parseSimpleTime(t, "2023-01-01 12:00:00"), "key4": parseSimpleTime(t, "2023-01-02 13:00:00")}
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_ts = ? WHERE name = ? AND age = ?`, overwriteMap, pkName, pkAge).Exec())

	addMap := map[string]time.Time{"key5": parseSimpleTime(t, "2023-01-01 12:00:00"), "key6": parseSimpleTime(t, "2023-01-02 13:00:00")}
	require.NoError(t, session.Query(`UPDATE user_info SET map_text_ts = map_text_ts + ? WHERE name = ? AND age = ?`, addMap, pkName, pkAge).Exec())

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_ts = map_text_ts - ? WHERE name = ? AND age = ?`, []string{"key3"}, pkName, pkAge).Exec())

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_ts['key1'] = ? WHERE name = ? AND age = ?`, parseSimpleTime(t, "2023-01-03 14:00:00"), pkName, pkAge).Exec())

	require.NoError(t, session.Query(`UPDATE user_info SET map_text_ts['key5'] = ? WHERE name = ? AND age = ?`, parseSimpleTime(t, "2023-01-04 15:00:00"), pkName, pkAge).Exec())

	var finalMap map[string]time.Time
	require.NoError(t, session.Query(`SELECT map_text_ts FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&finalMap))

	expectedMap := map[string]time.Time{
		"key1": parseSimpleTime(t, "2023-01-03 14:00:00"),
		"key4": parseSimpleTime(t, "2023-01-02 13:00:00"),
		"key5": parseSimpleTime(t, "2023-01-04 15:00:00"),
		"key6": parseSimpleTime(t, "2023-01-02 13:00:00"),
	}
	assert.Equal(t, expectedMap, finalMap)
}

func TestComplexOperationMapTimestampValueTypes(t *testing.T) {
	// Map<Timestamp, Boolean>
	pkNameBool, pkAgeBool := "Mia Smith", int64(30)
	initialMapBool := map[time.Time]bool{parseSimpleTime(t, "2023-01-01 12:00:00"): true, parseSimpleTime(t, "2023-01-02 13:00:00"): false}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_boolean_map) VALUES (?, ?, ?);`, pkNameBool, pkAgeBool, initialMapBool).Exec())
	addMapBool := map[time.Time]bool{parseSimpleTime(t, "2023-01-05 16:00:00"): true}
	require.NoError(t, session.Query(`UPDATE user_info SET ts_boolean_map = ts_boolean_map + ? WHERE name = ? AND age = ?`, addMapBool, pkNameBool, pkAgeBool).Exec())
	var finalMapBool map[time.Time]bool
	require.NoError(t, session.Query(`SELECT ts_boolean_map FROM user_info WHERE name = ? AND age = ?`, pkNameBool, pkAgeBool).Scan(&finalMapBool))
	expectedMapBool := map[time.Time]bool{
		parseSimpleTime(t, "2023-01-01 12:00:00"): true,
		parseSimpleTime(t, "2023-01-02 13:00:00"): false,
		parseSimpleTime(t, "2023-01-05 16:00:00"): true,
	}
	assert.Equal(t, expectedMapBool, finalMapBool, "Boolean map content mismatch")

	// Map<Timestamp, Text>
	pkNameText, pkAgeText := "Tom Smith", int64(40)
	initialMapText := map[time.Time]string{parseSimpleTime(t, "2023-01-01 12:00:00"): "value1"}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_text_map) VALUES (?, ?, ?);`, pkNameText, pkAgeText, initialMapText).Exec())
	addMapText := map[time.Time]string{parseSimpleTime(t, "2023-01-05 16:00:00"): "value5"}
	require.NoError(t, session.Query(`UPDATE user_info SET ts_text_map = ts_text_map + ? WHERE name = ? AND age = ?`, addMapText, pkNameText, pkAgeText).Exec())
	var finalMapText map[time.Time]string
	require.NoError(t, session.Query(`SELECT ts_text_map FROM user_info WHERE name = ? AND age = ?`, pkNameText, pkAgeText).Scan(&finalMapText))
	assert.Len(t, finalMapText, 2, "Expected 2 items in text map")

	// Map<Timestamp, Float>
	pkNameFloat, pkAgeFloat := "Noah Wilson", int64(32)
	initialMapFloat := map[time.Time]float32{parseSimpleTime(t, "2023-02-01 12:00:00"): 1.5}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_float_map) VALUES (?, ?, ?);`, pkNameFloat, pkAgeFloat, initialMapFloat).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET ts_float_map['2023-02-07 18:00:00'] = ? WHERE name = ? AND age = ?`, float32(7.75), pkNameFloat, pkAgeFloat).Exec())
	var finalMapFloat map[time.Time]float32
	require.NoError(t, session.Query(`SELECT ts_float_map FROM user_info WHERE name = ? AND age = ?`, pkNameFloat, pkAgeFloat).Scan(&finalMapFloat))
	assert.Len(t, finalMapFloat, 2, "Expected 2 items in float map")

	// Map<Timestamp, Double>
	pkNameDouble, pkAgeDouble := "Olivia Jackson", int64(33)
	initialMapDouble := map[time.Time]float64{parseSimpleTime(t, "2023-03-01 12:00:00"): 1.123}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_double_map) VALUES (?, ?, ?);`, pkNameDouble, pkAgeDouble, initialMapDouble).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET ts_double_map['2023-03-07 18:00:00'] = ? WHERE name = ? AND age = ?`, 7.777, pkNameDouble, pkAgeDouble).Exec())
	var finalMapDouble map[time.Time]float64
	require.NoError(t, session.Query(`SELECT ts_double_map FROM user_info WHERE name = ? AND age = ?`, pkNameDouble, pkAgeDouble).Scan(&finalMapDouble))
	assert.Len(t, finalMapDouble, 2, "Expected 2 items in double map")

	// Map<Timestamp, BigInt>
	pkNameBigInt, pkAgeBigInt := "Parker King", int64(34)
	initialMapBigInt := map[time.Time]int64{parseSimpleTime(t, "2023-04-01 12:00:00"): 1000000000}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_bigint_map) VALUES (?, ?, ?);`, pkNameBigInt, pkAgeBigInt, initialMapBigInt).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET ts_bigint_map['2023-04-07 18:00:00'] = ? WHERE name = ? AND age = ?`, int64(8000000000), pkNameBigInt, pkAgeBigInt).Exec())
	var finalMapBigInt map[time.Time]int64
	require.NoError(t, session.Query(`SELECT ts_bigint_map FROM user_info WHERE name = ? AND age = ?`, pkNameBigInt, pkAgeBigInt).Scan(&finalMapBigInt))
	assert.Len(t, finalMapBigInt, 2, "Expected 2 items in bigint map")

	// Map<Timestamp, Timestamp>
	pkNameTs, pkAgeTs := "Quinn Lewis", int64(35)
	initialMapTs := map[time.Time]time.Time{parseSimpleTime(t, "2023-05-01 12:00:00"): parseSimpleTime(t, "2023-06-01 08:00:00")}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_ts_map) VALUES (?, ?, ?);`, pkNameTs, pkAgeTs, initialMapTs).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET ts_ts_map['2023-05-07 18:00:00'] = ? WHERE name = ? AND age = ?`, parseSimpleTime(t, "2023-06-08 15:00:00"), pkNameTs, pkAgeTs).Exec())
	var finalMapTs map[time.Time]time.Time
	require.NoError(t, session.Query(`SELECT ts_ts_map FROM user_info WHERE name = ? AND age = ?`, pkNameTs, pkAgeTs).Scan(&finalMapTs))
	assert.Len(t, finalMapTs, 2, "Expected 2 items in timestamp map")

	// Map<Timestamp, Int>
	pkNameInt, pkAgeInt := "Riley Martinez", int64(36)
	initialMapInt := map[time.Time]int{parseSimpleTime(t, "2023-07-01 12:00:00"): 100}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, ts_int_map) VALUES (?, ?, ?);`, pkNameInt, pkAgeInt, initialMapInt).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET ts_int_map['2023-07-07 18:00:00'] = ? WHERE name = ? AND age = ?`, 700, pkNameInt, pkAgeInt).Exec())
	var finalMapInt map[time.Time]int
	require.NoError(t, session.Query(`SELECT ts_int_map FROM user_info WHERE name = ? AND age = ?`, pkNameInt, pkAgeInt).Scan(&finalMapInt))
	assert.Len(t, finalMapInt, 2, "Expected 2 items in int map")
}

func TestComplexOperationListTypes(t *testing.T) {
	// LIST<TIMESTAMP>
	pkNameTs, pkAgeTs := "Riley Martinez", int64(36)
	initialListTs := []time.Time{parseSimpleTime(t, "2023-07-01 12:00:00"), parseSimpleTime(t, "2023-07-02 13:00:00")}
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_timestamp) VALUES (?, ?, ?);`, pkNameTs, pkAgeTs, initialListTs).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_timestamp = list_timestamp + ? WHERE name = ? AND age = ?`, []time.Time{parseSimpleTime(t, "2023-07-03 14:00:00")}, pkNameTs, pkAgeTs).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_timestamp = list_timestamp - ? WHERE name = ? AND age = ?`, []time.Time{parseSimpleTime(t, "2023-07-01 12:00:00")}, pkNameTs, pkAgeTs).Exec())
	var finalListTs []time.Time
	require.NoError(t, session.Query(`SELECT list_timestamp FROM user_info WHERE name = ? AND age = ?`, pkNameTs, pkAgeTs).Scan(&finalListTs))
	assert.Equal(t, []time.Time{parseSimpleTime(t, "2023-07-02 13:00:00"), parseSimpleTime(t, "2023-07-03 14:00:00")}, finalListTs)

	// LIST<TEXT>
	pkNameText, pkAgeText := "Sophia Taylor", int64(28)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_text) VALUES (?, ?, ?);`, pkNameText, pkAgeText, []string{"apple", "banana"}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_text = list_text + ? WHERE name = ? AND age = ?`, []string{"orange"}, pkNameText, pkAgeText).Exec())
	var finalListString []string
	require.NoError(t, session.Query(`SELECT list_text FROM user_info WHERE name = ? AND age = ?`, pkNameText, pkAgeText).Scan(&finalListString))
	assert.Equal(t, []string{"apple", "banana", "orange"}, finalListString)

	// LIST<INT>
	pkNameInt, pkAgeInt := "Thomas Walker", int64(29)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_int) VALUES (?, ?, ?);`, pkNameInt, pkAgeInt, []int{10, 20}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_int = list_int - ? WHERE name = ? AND age = ?`, []int{10}, pkNameInt, pkAgeInt).Exec())
	var finalLintInt []int
	require.NoError(t, session.Query(`SELECT list_int FROM user_info WHERE name = ? AND age = ?`, pkNameInt, pkAgeInt).Scan(&finalLintInt))
	assert.Equal(t, []int{20}, finalLintInt)

	// LIST<BIGINT>
	pkNameBigInt, pkAgeBigInt := "Ursula Garcia", int64(30)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_bigint) VALUES (?, ?, ?);`, pkNameBigInt, pkAgeBigInt, []int64{1000000000, 2000000000}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_bigint = list_bigint + ? WHERE name = ? AND age = ?`, []int64{3000000000}, pkNameBigInt, pkAgeBigInt).Exec())
	var finalListBigInt []int64
	require.NoError(t, session.Query(`SELECT list_bigint FROM user_info WHERE name = ? AND age = ?`, pkNameBigInt, pkAgeBigInt).Scan(&finalListBigInt))
	assert.Equal(t, []int64{1000000000, 2000000000, 3000000000}, finalListBigInt)

	// LIST<FLOAT>
	pkNameFloat, pkAgeFloat := "Victoria Nelson", int64(31)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_float) VALUES (?, ?, ?);`, pkNameFloat, pkAgeFloat, []float32{1.5, 2.75}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_float = list_float + ? WHERE name = ? AND age = ?`, []float32{3.25}, pkNameFloat, pkAgeFloat).Exec())
	var finalListFloat []float32
	require.NoError(t, session.Query(`SELECT list_float FROM user_info WHERE name = ? AND age = ?`, pkNameFloat, pkAgeFloat).Scan(&finalListFloat))
	assert.Equal(t, []float32{1.5, 2.75, 3.25}, finalListFloat)

	// LIST<DOUBLE>
	pkNameDouble, pkAgeDouble := "William Davis", int64(32)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_double) VALUES (?, ?, ?);`, pkNameDouble, pkAgeDouble, []float64{1.123, 2.987}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_double = list_double - ? WHERE name = ? AND age = ?`, []float64{1.123}, pkNameDouble, pkAgeDouble).Exec())
	var finalListDouble []float64
	require.NoError(t, session.Query(`SELECT list_double FROM user_info WHERE name = ? AND age = ?`, pkNameDouble, pkAgeDouble).Scan(&finalListDouble))
	assert.Equal(t, []float64{2.987}, finalListDouble)

	// LIST<BOOLEAN>
	pkNameBool, pkAgeBool := "Xavier Thompson", int64(33)
	require.NoError(t, session.Query(`INSERT INTO user_info(name, age, list_boolean) VALUES (?, ?, ?);`, pkNameBool, pkAgeBool, []bool{true, false}).Exec())
	require.NoError(t, session.Query(`UPDATE user_info SET list_boolean = list_boolean + ? WHERE name = ? AND age = ?`, []bool{true}, pkNameBool, pkAgeBool).Exec())
	var finalListBool []bool
	require.NoError(t, session.Query(`SELECT list_boolean FROM user_info WHERE name = ? AND age = ?`, pkNameBool, pkAgeBool).Scan(&finalListBool))
	assert.Equal(t, []bool{true, false, true}, finalListBool)
}
