package domain

import (
	"strings"
	"testing"
)

const sqliteFixture = `{"ordered":false,"tables":[{"name":"users","columns":[{"name":"id","type":"INTEGER"},{"name":"name","type":"TEXT"}],"rows":[[1,"ann"],[2,null]]}]}`

// TestParseSQLiteFixture проверяет формат набора данных, совместимый с sandbox.
func TestParseSQLiteFixture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid", input: sqliteFixture},
		{name: "empty tables", input: `{"ordered":true,"tables":[]}`},
		{name: "missing ordered", input: `{"tables":[]}`, wantErr: true},
		{name: "unknown field", input: `{"ordered":false,"tables":[],"extra":1}`, wantErr: true},
		{name: "sqlite table", input: `{"ordered":false,"tables":[{"name":"sqlite_master","columns":[{"name":"id","type":"INTEGER"}],"rows":[]}]}`, wantErr: true},
		{name: "duplicate column", input: `{"ordered":false,"tables":[{"name":"t","columns":[{"name":"id","type":"INTEGER"},{"name":"ID","type":"INTEGER"}],"rows":[]}]}`, wantErr: true},
		{name: "bad type", input: `{"ordered":false,"tables":[{"name":"t","columns":[{"name":"id","type":"BOOL"}],"rows":[]}]}`, wantErr: true},
		{name: "text into integer", input: `{"ordered":false,"tables":[{"name":"t","columns":[{"name":"id","type":"INTEGER"}],"rows":[["one"]]}]}`, wantErr: true},
		{name: "row width", input: `{"ordered":false,"tables":[{"name":"t","columns":[{"name":"id","type":"INTEGER"}],"rows":[[1,2]]}]}`, wantErr: true},
		{name: "trailing data", input: sqliteFixture + `{}`, wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ordered, err := ParseSQLiteFixture(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("ParseSQLiteFixture() error = %v, wantErr = %v", err, test.wantErr)
			}
			if err == nil && test.name == "valid" && ordered {
				t.Fatal("ordered flag lost")
			}
		})
	}
}

// TestSQLiteRowsMatch проверяет учёт дублей и настройку порядка.
func TestSQLiteRowsMatch(t *testing.T) {
	t.Parallel()

	const expected = `[ [1,"ann"], [2,null], [1,"ann"] ]`
	if match, err := SQLiteRowsMatch(`[[2,null],[1,"ann"],[1,"ann"]]`, expected, false); err != nil || !match {
		t.Fatalf("unordered rows: match=%v err=%v", match, err)
	}
	if match, err := SQLiteRowsMatch(`[[2,null],[1,"ann"],[1,"ann"]]`, expected, true); err != nil || match {
		t.Fatalf("ordered rows accepted in wrong order: match=%v err=%v", match, err)
	}
	if match, _ := SQLiteRowsMatch(`[[1,"ann"],[1,"ann"]]`, expected, false); match {
		t.Fatal("lost duplicate row accepted")
	}
	if match, _ := SQLiteRowsMatch(`not json`, expected, false); match {
		t.Fatal("invalid output accepted")
	}
	if _, err := SQLiteRowsMatch(`[]`, `not json`, false); err == nil {
		t.Fatal("invalid expected rows not reported")
	}
}

// TestSQLiteQuerySizeMatchesSandbox ограничивает запрос лимитом адаптера.
func TestSQLiteQuerySizeMatchesSandbox(t *testing.T) {
	t.Parallel()

	if err := ValidateSourceFile(ProgrammingLanguageSQLite, "solution.sql", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("x", MaxSQLiteQueryBytes+1)
	if ValidateSourceFile(ProgrammingLanguageSQLite, "solution.sql", large) == nil {
		t.Fatal("query above sandbox limit accepted")
	}
}
