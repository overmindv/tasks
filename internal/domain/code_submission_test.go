package domain

import (
	"strings"
	"testing"
)

// TestValidateSourceFile проверяет allowlist языка, расширение и бинарное содержимое.
func TestValidateSourceFile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		language ProgrammingLanguage
		fileName string
		code     string
		wantErr  bool
	}{
		{name: "python", language: ProgrammingLanguagePython, fileName: "main.py", code: "print(input())"},
		{name: "go", language: ProgrammingLanguageGo, fileName: "main.go", code: "package main"},
		{name: "wrong extension", language: ProgrammingLanguagePython, fileName: "main.go", code: "print()", wantErr: true},
		{name: "unknown language", language: ProgrammingLanguage("javascript"), fileName: "main.js", code: "console.log()", wantErr: true},
		{name: "nul byte", language: ProgrammingLanguageGo, fileName: "main.go", code: "package\x00main", wantErr: true},
		{name: "too large", language: ProgrammingLanguagePython, fileName: "main.py", code: strings.Repeat("x", MaxSourceFileSize+1), wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateSourceFile(test.language, test.fileName, test.code)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateSourceFile() error = %v, wantErr = %v", err, test.wantErr)
			}
		})
	}
}

// TestNormalizeSourceFileDropsClientPath проверяет удаление пути из multipart filename.
func TestNormalizeSourceFileDropsClientPath(t *testing.T) {
	t.Parallel()
	name, code := NormalizeSourceFile(`C:\\users\\student\\main.py`, "print(1)")
	if name != "main.py" || code != "print(1)" {
		t.Fatalf("NormalizeSourceFile() = %q, %q", name, code)
	}
}
