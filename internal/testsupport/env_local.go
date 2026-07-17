package testsupport

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LoadRepositoryEnvLocal loads repository-root env.local when it exists.
// Existing process environment variables always take precedence.
//
// The file is intentionally loaded only when a caller opts in to this helper;
// importing testsupport never changes process environment state by itself.
func LoadRepositoryEnvLocal() error {
	root, found, err := repositoryRoot()
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	return loadEnvFile(filepath.Join(root, "env.local"))
}

func repositoryRoot() (string, bool, error) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return "", false, fmt.Errorf("find repository root: %w", err)
	}

	for directory := filepath.Clean(workingDirectory); ; directory = filepath.Dir(directory) {
		if info, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil && !info.IsDir() {
			if _, err := os.Stat(filepath.Join(directory, "env.local")); err != nil {
				if os.IsNotExist(err) {
					return directory, false, nil
				}
				return "", false, fmt.Errorf("inspect repository env.local: %w", err)
			}
			return directory, true, nil
		}

		parent := filepath.Dir(directory)
		if parent == directory {
			return "", false, nil
		}
	}
}

func loadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open env.local: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lineNumber int
	for scanner.Scan() {
		lineNumber++
		key, value, ok, err := parseEnvLine(scanner.Text())
		if err != nil {
			return fmt.Errorf("parse env.local line %d: %w", lineNumber, err)
		}
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set env.local variable %q: %w", key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read env.local: %w", err)
	}
	return nil
}

func parseEnvLine(line string) (key, value string, ok bool, err error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}
	if strings.HasPrefix(line, "export ") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
	}

	separator := strings.IndexByte(line, '=')
	if separator <= 0 {
		return "", "", false, fmt.Errorf("expected KEY=value")
	}
	key = strings.TrimSpace(line[:separator])
	if !validEnvKey(key) {
		return "", "", false, fmt.Errorf("invalid variable name %q", key)
	}

	rawValue := strings.TrimSpace(line[separator+1:])
	if rawValue == "" {
		return key, "", true, nil
	}
	if rawValue[0] == '\'' {
		if len(rawValue) < 2 || rawValue[len(rawValue)-1] != '\'' {
			return "", "", false, fmt.Errorf("unterminated single-quoted value for %q", key)
		}
		return key, rawValue[1 : len(rawValue)-1], true, nil
	}
	if rawValue[0] == '"' {
		if len(rawValue) < 2 || rawValue[len(rawValue)-1] != '"' {
			return "", "", false, fmt.Errorf("unterminated double-quoted value for %q", key)
		}
		value, err := strconv.Unquote(rawValue)
		if err != nil {
			return "", "", false, fmt.Errorf("invalid double-quoted value for %q", key)
		}
		return key, value, true, nil
	}
	return key, rawValue, true, nil
}

func validEnvKey(key string) bool {
	for index, character := range key {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(index > 0 && character >= '0' && character <= '9') ||
			(index > 0 && character == '_') {
			continue
		}
		return false
	}
	return key != ""
}
