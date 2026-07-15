package protocol

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"unicode/utf8"
)

// CanonicalJSON encodes the restricted Protocol V1 JSON value set without
// whitespace, a trailing newline, or implementation-specific escaping.
func CanonicalJSON(value any) ([]byte, error) {
	var out bytes.Buffer
	if err := encodeCanonicalJSON(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// DecodeCanonicalJSON parses and validates one canonical Protocol V1 JSON
// document. It rejects duplicate keys, floating point numbers, invalid UTF-8,
// and any byte representation that is not the canonical encoding of its value.
func DecodeCanonicalJSON(data []byte) (any, error) {
	if bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}) {
		return nil, fmt.Errorf("canonical JSON must not contain a BOM")
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("canonical JSON is not valid UTF-8")
	}
	parser := jsonParser{data: data}
	value, err := parser.parseValue()
	if err != nil {
		return nil, err
	}
	parser.skipWhitespace()
	if parser.pos != len(data) {
		return nil, fmt.Errorf("canonical JSON has trailing bytes at offset %d", parser.pos)
	}
	canonical, err := CanonicalJSON(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, data) {
		return nil, fmt.Errorf("canonical JSON bytes are not canonical")
	}
	return value, nil
}

func encodeCanonicalJSON(out *bytes.Buffer, value any) error {
	if value == nil {
		out.WriteString("null")
		return nil
	}
	switch typed := value.(type) {
	case bool:
		if typed {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
		return nil
	case string:
		return encodeCanonicalJSONString(out, typed)
	case []byte:
		return fmt.Errorf("canonical JSON does not accept []byte")
	case []any:
		out.WriteByte('[')
		for i, item := range typed {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := encodeCanonicalJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
		return nil
	case map[string]any:
		return encodeCanonicalJSONObject(out, typed)
	case float32, float64:
		return fmt.Errorf("canonical JSON accepts integers only")
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out.WriteString(strconv.FormatInt(rv.Int(), 10))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		out.WriteString(strconv.FormatUint(rv.Uint(), 10))
		return nil
	case reflect.Float32, reflect.Float64:
		if math.IsNaN(rv.Float()) || math.IsInf(rv.Float(), 0) {
			return fmt.Errorf("canonical JSON accepts integers only")
		}
		return fmt.Errorf("canonical JSON accepts integers only")
	case reflect.Slice, reflect.Array:
		out.WriteByte('[')
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := encodeCanonicalJSON(out, rv.Index(i).Interface()); err != nil {
				return err
			}
		}
		out.WriteByte(']')
		return nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("canonical JSON object keys must be strings")
		}
		object := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			object[iter.Key().String()] = iter.Value().Interface()
		}
		return encodeCanonicalJSONObject(out, object)
	default:
		return fmt.Errorf("unsupported canonical JSON value %T", value)
	}
}

func encodeCanonicalJSONObject(out *bytes.Buffer, object map[string]any) error {
	keys := make([]string, 0, len(object))
	for key := range object {
		if !utf8.ValidString(key) {
			return fmt.Errorf("canonical JSON object key is not valid UTF-8")
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0 })
	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		if err := encodeCanonicalJSONString(out, key); err != nil {
			return err
		}
		out.WriteByte(':')
		if err := encodeCanonicalJSON(out, object[key]); err != nil {
			return err
		}
	}
	out.WriteByte('}')
	return nil
}

func encodeCanonicalJSONString(out *bytes.Buffer, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("canonical JSON string is not valid UTF-8")
	}
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			switch {
			case r < 0x20:
				writeUnicodeEscape(out, uint16(r))
			case r <= 0x7f:
				out.WriteByte(byte(r))
			case r <= 0xffff:
				writeUnicodeEscape(out, uint16(r))
			default:
				run := r - 0x10000
				writeUnicodeEscape(out, uint16(0xd800+(run>>10)))
				writeUnicodeEscape(out, uint16(0xdc00+(run&0x3ff)))
			}
		}
	}
	out.WriteByte('"')
	return nil
}

func writeUnicodeEscape(out *bytes.Buffer, value uint16) {
	const hex = "0123456789abcdef"
	out.WriteString(`\u`)
	out.WriteByte(hex[(value>>12)&0xf])
	out.WriteByte(hex[(value>>8)&0xf])
	out.WriteByte(hex[(value>>4)&0xf])
	out.WriteByte(hex[value&0xf])
}

type jsonParser struct {
	data []byte
	pos  int
}

func (p *jsonParser) skipWhitespace() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\r', '\n':
			p.pos++
		default:
			return
		}
	}
}

func (p *jsonParser) parseValue() (any, error) {
	p.skipWhitespace()
	if p.pos >= len(p.data) {
		return nil, fmt.Errorf("canonical JSON ended before a value")
	}
	switch p.data[p.pos] {
	case 'n':
		return p.parseLiteral("null", nil)
	case 't':
		return p.parseLiteral("true", true)
	case 'f':
		return p.parseLiteral("false", false)
	case '"':
		return p.parseString()
	case '[':
		return p.parseArray()
	case '{':
		return p.parseObject()
	default:
		if p.data[p.pos] == '-' || (p.data[p.pos] >= '0' && p.data[p.pos] <= '9') {
			return p.parseInteger()
		}
		return nil, fmt.Errorf("invalid JSON value at offset %d", p.pos)
	}
}

func (p *jsonParser) parseLiteral(literal string, value any) (any, error) {
	if !bytes.HasPrefix(p.data[p.pos:], []byte(literal)) {
		return nil, fmt.Errorf("invalid JSON literal at offset %d", p.pos)
	}
	p.pos += len(literal)
	return value, nil
}

func (p *jsonParser) parseArray() ([]any, error) {
	p.pos++
	result := []any{}
	p.skipWhitespace()
	if p.consume(']') {
		return result, nil
	}
	for {
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		p.skipWhitespace()
		if p.consume(']') {
			return result, nil
		}
		if !p.consume(',') {
			return nil, fmt.Errorf("expected comma in JSON array at offset %d", p.pos)
		}
	}
}

func (p *jsonParser) parseObject() (map[string]any, error) {
	p.pos++
	result := make(map[string]any)
	p.skipWhitespace()
	if p.consume('}') {
		return result, nil
	}
	for {
		p.skipWhitespace()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			return nil, fmt.Errorf("object key must be a JSON string at offset %d", p.pos)
		}
		keyValue, err := p.parseString()
		if err != nil {
			return nil, err
		}
		key := keyValue
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate JSON object key %q", key)
		}
		p.skipWhitespace()
		if !p.consume(':') {
			return nil, fmt.Errorf("expected colon after JSON object key at offset %d", p.pos)
		}
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		result[key] = value
		p.skipWhitespace()
		if p.consume('}') {
			return result, nil
		}
		if !p.consume(',') {
			return nil, fmt.Errorf("expected comma in JSON object at offset %d", p.pos)
		}
	}
}

func (p *jsonParser) parseString() (string, error) {
	if !p.consume('"') {
		return "", fmt.Errorf("expected JSON string at offset %d", p.pos)
	}
	var out bytes.Buffer
	for p.pos < len(p.data) {
		b := p.data[p.pos]
		p.pos++
		switch b {
		case '"':
			return out.String(), nil
		case '\\':
			if p.pos >= len(p.data) {
				return "", fmt.Errorf("unterminated JSON escape")
			}
			escape := p.data[p.pos]
			p.pos++
			switch escape {
			case '"', '\\', '/':
				out.WriteByte(escape)
			case 'b':
				out.WriteByte('\b')
			case 'f':
				out.WriteByte('\f')
			case 'n':
				out.WriteByte('\n')
			case 'r':
				out.WriteByte('\r')
			case 't':
				out.WriteByte('\t')
			case 'u':
				codeUnit, err := p.parseHex4()
				if err != nil {
					return "", err
				}
				if codeUnit >= 0xd800 && codeUnit <= 0xdbff {
					if p.pos+6 > len(p.data) || p.data[p.pos] != '\\' || p.data[p.pos+1] != 'u' {
						return "", fmt.Errorf("unpaired high surrogate in JSON string")
					}
					p.pos += 2
					low, err := p.parseHex4()
					if err != nil || low < 0xdc00 || low > 0xdfff {
						return "", fmt.Errorf("invalid UTF-16 surrogate pair in JSON string")
					}
					r := rune(0x10000 + ((uint32(codeUnit) - 0xd800) << 10) + uint32(low) - 0xdc00)
					out.WriteRune(r)
				} else if codeUnit >= 0xdc00 && codeUnit <= 0xdfff {
					return "", fmt.Errorf("unpaired low surrogate in JSON string")
				} else {
					out.WriteRune(rune(codeUnit))
				}
			default:
				return "", fmt.Errorf("invalid JSON escape at offset %d", p.pos-1)
			}
		case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x0b, 0x0c, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
			0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
			0x1e, 0x1f:
			return "", fmt.Errorf("unescaped JSON control character at offset %d", p.pos-1)
		default:
			if b < utf8.RuneSelf {
				out.WriteByte(b)
				continue
			}
			p.pos--
			r, size := utf8.DecodeRune(p.data[p.pos:])
			if r == utf8.RuneError && size == 1 {
				return "", fmt.Errorf("invalid UTF-8 in JSON string at offset %d", p.pos)
			}
			out.Write(p.data[p.pos : p.pos+size])
			p.pos += size
		}
	}
	return "", fmt.Errorf("unterminated JSON string")
}

func (p *jsonParser) parseHex4() (uint16, error) {
	if p.pos+4 > len(p.data) {
		return 0, fmt.Errorf("truncated JSON unicode escape")
	}
	var value uint16
	for i := 0; i < 4; i++ {
		digit, ok := hexDigit(p.data[p.pos+i])
		if !ok {
			return 0, fmt.Errorf("invalid JSON unicode escape at offset %d", p.pos+i)
		}
		value = value<<4 | uint16(digit)
	}
	p.pos += 4
	return value, nil
}

func hexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func (p *jsonParser) parseInteger() (any, error) {
	start := p.pos
	if p.data[p.pos] == '-' {
		p.pos++
		if p.pos >= len(p.data) {
			return nil, fmt.Errorf("truncated JSON integer")
		}
	}
	if p.data[p.pos] == '0' {
		p.pos++
		if p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			return nil, fmt.Errorf("JSON integer has a leading zero")
		}
	} else if p.data[p.pos] >= '1' && p.data[p.pos] <= '9' {
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
	} else {
		return nil, fmt.Errorf("invalid JSON integer at offset %d", p.pos)
	}
	if p.pos < len(p.data) && (p.data[p.pos] == '.' || p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		return nil, fmt.Errorf("canonical JSON accepts integers only")
	}
	raw := string(p.data[start:p.pos])
	if raw == "-0" {
		return nil, fmt.Errorf("negative zero is not canonical JSON")
	}
	if raw[0] == '-' {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("JSON integer is outside signed 64-bit range")
		}
		return value, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("JSON integer is outside unsigned 64-bit range")
	}
	return value, nil
}

func (p *jsonParser) consume(want byte) bool {
	if p.pos < len(p.data) && p.data[p.pos] == want {
		p.pos++
		return true
	}
	return false
}
