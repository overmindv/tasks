package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	// SQLiteChecker сравнивает строки результата с учётом дублей.
	SQLiteChecker     = "sqlite_rows"
	sqliteTypeInteger = "INTEGER"
	sqliteTypeReal    = "REAL"
	sqliteTypeText    = "TEXT"
	// MaxSQLiteQueryBytes совпадает с лимитом sandbox на текст SQL-запроса.
	MaxSQLiteQueryBytes = 64 * 1024

	maxSQLiteFixtureBytes = 65536
	maxSQLiteTables       = 16
	maxSQLiteColumns      = 64
	maxSQLiteFixtureRows  = 10000
	maxSQLiteResultRows   = 4096
)

var sqliteIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// SQLiteColumn — колонка учебного набора.
type SQLiteColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SQLiteTable — таблица учебного набора кейса.
type SQLiteTable struct {
	Name    string              `json:"name"`
	Columns []SQLiteColumn      `json:"columns"`
	Rows    [][]json.RawMessage `json:"rows"`
}

// ParseSQLiteFixture строго проверяет набор данных и возвращает требование порядка строк.
// Формат совпадает с runner'ом sandbox: {"ordered": bool, "tables": [...]}.
func ParseSQLiteFixture(input string) (bool, error) {
	if input == "" || len(input) > maxSQLiteFixtureBytes {
		return false, errors.New("invalid sqlite fixture size")
	}
	// Указатели фиксируют обязательные поля: sandbox отклоняет набор без них.
	var fixture struct {
		Ordered *bool          `json:"ordered"`
		Tables  *[]SQLiteTable `json:"tables"`
	}
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		return false, errors.New("invalid sqlite fixture")
	}
	if !decoderEnds(decoder) || fixture.Ordered == nil || fixture.Tables == nil || len(*fixture.Tables) > maxSQLiteTables {
		return false, errors.New("invalid sqlite fixture")
	}

	names := make(map[string]struct{}, len(*fixture.Tables))
	rowCount := 0
	for _, table := range *fixture.Tables {
		name := strings.ToLower(table.Name)
		if !sqliteIdentifier.MatchString(table.Name) || strings.HasPrefix(name, "sqlite_") {
			return false, errors.New("invalid sqlite table name")
		}
		if _, duplicate := names[name]; duplicate {
			return false, errors.New("duplicate sqlite table")
		}
		names[name] = struct{}{}
		if len(table.Columns) < 1 || len(table.Columns) > maxSQLiteColumns {
			return false, errors.New("invalid sqlite columns")
		}
		columns := make(map[string]struct{}, len(table.Columns))
		kinds := make([]string, len(table.Columns))
		for index, column := range table.Columns {
			columnName := strings.ToLower(column.Name)
			if !sqliteIdentifier.MatchString(column.Name) {
				return false, errors.New("invalid sqlite column name")
			}
			if _, duplicate := columns[columnName]; duplicate {
				return false, errors.New("duplicate sqlite column")
			}
			columns[columnName] = struct{}{}
			if column.Type != sqliteTypeInteger && column.Type != sqliteTypeReal && column.Type != sqliteTypeText {
				return false, errors.New("invalid sqlite column type")
			}
			kinds[index] = column.Type
		}
		rowCount += len(table.Rows)
		if rowCount > maxSQLiteFixtureRows {
			return false, errors.New("too many sqlite fixture rows")
		}
		for _, row := range table.Rows {
			if len(row) != len(kinds) {
				return false, errors.New("invalid sqlite row width")
			}
			for index, raw := range row {
				if !validSQLiteFixtureCell(raw, kinds[index]) {
					return false, errors.New("invalid sqlite cell")
				}
			}
		}
	}

	return *fixture.Ordered, nil
}

// ValidateSQLiteRows проверяет эталон результата: JSON-массив строк.
func ValidateSQLiteRows(output string) error {
	_, err := parseSQLiteRows(output)

	return err
}

// SQLiteRowsMatch сравнивает вывод sandbox с эталоном.
// Дубли учитываются, порядок проверяется только при ordered=true.
// Ошибка означает невалидный эталон; невалидный вывод считается несовпадением.
func SQLiteRowsMatch(actual, expected string, ordered bool) (bool, error) {
	expectedRows, err := parseSQLiteRows(expected)
	if err != nil {
		return false, err
	}
	actualRows, err := parseSQLiteRows(actual)
	if err != nil {
		return false, nil
	}
	if len(actualRows) != len(expectedRows) {
		return false, nil
	}
	if ordered {
		for index := range expectedRows {
			if actualRows[index] != expectedRows[index] {
				return false, nil
			}
		}

		return true, nil
	}
	sort.Strings(actualRows)
	sort.Strings(expectedRows)
	for index := range expectedRows {
		if actualRows[index] != expectedRows[index] {
			return false, nil
		}
	}

	return true, nil
}

// parseSQLiteRows возвращает канонические JSON-строки для сравнения.
func parseSQLiteRows(output string) ([]string, error) {
	if output == "" || len(output) > maxSQLiteFixtureBytes {
		return nil, errors.New("invalid sqlite rows size")
	}
	decoder := json.NewDecoder(strings.NewReader(output))
	decoder.UseNumber()
	var rows [][]json.RawMessage
	if err := decoder.Decode(&rows); err != nil {
		return nil, errors.New("invalid sqlite rows")
	}
	if !decoderEnds(decoder) || rows == nil {
		return nil, errors.New("invalid sqlite rows")
	}
	if len(rows) > maxSQLiteResultRows {
		return nil, errors.New("too many sqlite rows")
	}
	canonical := make([]string, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			return nil, errors.New("invalid sqlite row")
		}
		for _, cell := range row {
			if !validSQLiteResultCell(cell) {
				return nil, errors.New("invalid sqlite cell")
			}
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, errors.New("invalid sqlite row")
		}
		canonical = append(canonical, string(encoded))
	}

	return canonical, nil
}

// validSQLiteFixtureCell повторяет правила runner'а sandbox для колонок набора.
func validSQLiteFixtureCell(raw json.RawMessage, kind string) bool {
	if isJSONNull(raw) {
		return true
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return kind == sqliteTypeText
	}
	number, ok := jsonNumber(raw)
	if !ok {
		return false
	}
	if strings.ContainsAny(number, ".eE") {
		value, err := strconv.ParseFloat(number, 64)

		return err == nil && !math.IsInf(value, 0) && !math.IsNaN(value) && kind == sqliteTypeReal
	}
	_, err := strconv.ParseInt(number, 10, 64)

	return err == nil && (kind == sqliteTypeInteger || kind == sqliteTypeReal)
}

// validSQLiteResultCell разрешает только JSON-скаляры, которые выводит sandbox.
func validSQLiteResultCell(raw json.RawMessage) bool {
	if isJSONNull(raw) {
		return true
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return true
	}
	number, ok := jsonNumber(raw)
	if !ok {
		return false
	}
	if strings.ContainsAny(number, ".eE") {
		value, err := strconv.ParseFloat(number, 64)

		return err == nil && !math.IsInf(value, 0) && !math.IsNaN(value)
	}
	_, err := strconv.ParseInt(number, 10, 64)

	return err == nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// jsonNumber возвращает исходный текст числа без потери точности.
func jsonNumber(raw json.RawMessage) (string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var number json.Number
	if err := decoder.Decode(&number); err != nil {
		return "", false
	}
	if !decoderEnds(decoder) {
		return "", false
	}

	return number.String(), true
}

// decoderEnds проверяет отсутствие хвоста после JSON-значения.
func decoderEnds(decoder *json.Decoder) bool {
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}
