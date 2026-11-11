package main

import "strings"

// splitSQLStatements splits a PostgreSQL migration script into individual
// statements. It respects:
//   - single-quoted strings ('...' with ” escape)
//   - dollar-quoted strings ($tag$ ... $tag$, including the empty $$ tag)
//   - line comments (-- ... newline)
//   - block comments (/* ... */, nesting per PostgreSQL semantics)
//
// Statements are separated by ';' at the top level (i.e. outside any of the
// above). Empty / whitespace-only segments are discarded.
//
// The splitter is intentionally lenient: it does not validate SQL, only
// tokenizes far enough to know when a ';' terminates a statement vs. lives
// inside a literal. That is the minimum needed because pgx/v5's stdlib
// extended protocol does not accept multi-statement Exec — earlier
// migrations would silently lose every statement after the first.
func splitSQLStatements(src string) []string {
	var out []string
	var buf strings.Builder
	n := len(src)

	flush := func() {
		s := strings.TrimSpace(buf.String())
		if s != "" {
			out = append(out, s)
		}
		buf.Reset()
	}

	i := 0
	for i < n {
		c := src[i]

		// line comment: skip until newline, drop comment, keep the newline
		if c == '-' && i+1 < n && src[i+1] == '-' {
			for i < n && src[i] != '\n' {
				i++
			}
			if i < n {
				buf.WriteByte('\n')
				i++
			}
			continue
		}

		// block comment: /* ... */, nestable in PG
		if c == '/' && i+1 < n && src[i+1] == '*' {
			depth := 1
			i += 2
			for i < n && depth > 0 {
				switch {
				case i+1 < n && src[i] == '/' && src[i+1] == '*':
					depth++
					i += 2
				case i+1 < n && src[i] == '*' && src[i+1] == '/':
					depth--
					i += 2
				default:
					i++
				}
			}
			buf.WriteByte(' ')
			continue
		}

		// single-quoted string: copy verbatim, honor '' escape
		if c == '\'' {
			buf.WriteByte(c)
			i++
			for i < n {
				if src[i] == '\'' {
					if i+1 < n && src[i+1] == '\'' {
						buf.WriteByte('\'')
						buf.WriteByte('\'')
						i += 2
						continue
					}
					buf.WriteByte('\'')
					i++
					break
				}
				buf.WriteByte(src[i])
				i++
			}
			continue
		}

		// dollar-quoted string: $tag$ ... $tag$
		if c == '$' {
			j := i + 1
			for j < n && isTagByte(src[j]) {
				j++
			}
			if j < n && src[j] == '$' {
				tag := src[i : j+1]
				buf.WriteString(tag)
				i = j + 1
				for i < n {
					if i+len(tag) <= n && src[i:i+len(tag)] == tag {
						buf.WriteString(tag)
						i += len(tag)
						break
					}
					buf.WriteByte(src[i])
					i++
				}
				continue
			}
			// $ that does not open a dollar-quote (e.g. $1 positional param) —
			// fall through and treat as a normal byte.
		}

		// statement terminator
		if c == ';' {
			flush()
			i++
			continue
		}

		buf.WriteByte(c)
		i++
	}
	flush()
	return out
}

func isTagByte(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_'
}
