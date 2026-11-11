package main

import (
	"reflect"
	"testing"
)

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "two simple statements",
			in:   "CREATE TABLE a (id int); CREATE TABLE b (id int);",
			want: []string{
				"CREATE TABLE a (id int)",
				"CREATE TABLE b (id int)",
			},
		},
		{
			name: "trailing statement without semicolon",
			in:   "SELECT 1;\nSELECT 2",
			want: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name: "line comments stripped, statements preserved",
			in: `-- header
CREATE TABLE a (id int); -- trailing
-- floating comment
CREATE TABLE b (id int);`,
			want: []string{
				"CREATE TABLE a (id int)",
				"CREATE TABLE b (id int)",
			},
		},
		{
			name: "semicolon inside single-quoted literal does not split",
			in:   "INSERT INTO t (s) VALUES ('a;b'); SELECT 1;",
			want: []string{
				"INSERT INTO t (s) VALUES ('a;b')",
				"SELECT 1",
			},
		},
		{
			name: "doubled-quote escape inside literal",
			in:   "INSERT INTO t (s) VALUES ('it''s; fine'); SELECT 1;",
			want: []string{
				"INSERT INTO t (s) VALUES ('it''s; fine')",
				"SELECT 1",
			},
		},
		{
			name: "empty dollar-quoted body protects semicolons",
			in: `CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1;
  PERFORM 2;
END;
$$;
SELECT 1;`,
			want: []string{
				"CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n  PERFORM 1;\n  PERFORM 2;\nEND;\n$$",
				"SELECT 1",
			},
		},
		{
			name: "tagged dollar-quote",
			in: `CREATE FUNCTION g() RETURNS text LANGUAGE sql AS $body$
  SELECT 'has ; semicolons; inside';
$body$;
SELECT 2;`,
			want: []string{
				"CREATE FUNCTION g() RETURNS text LANGUAGE sql AS $body$\n  SELECT 'has ; semicolons; inside';\n$body$",
				"SELECT 2",
			},
		},
		{
			name: "DO block",
			in: `DO $$
BEGIN
  IF NOT EXISTS (SELECT 1) THEN
    RAISE NOTICE 'a;b';
  END IF;
END;
$$;
SELECT 3;`,
			want: []string{
				"DO $$\nBEGIN\n  IF NOT EXISTS (SELECT 1) THEN\n    RAISE NOTICE 'a;b';\n  END IF;\nEND;\n$$",
				"SELECT 3",
			},
		},
		{
			name: "block comment with nested /* */",
			in:   "/* outer /* inner; */ still comment; */ SELECT 1;",
			want: []string{"SELECT 1"},
		},
		{
			name: "empty input",
			in:   "",
			want: nil,
		},
		{
			name: "whitespace and comments only",
			in: `-- only comments
/* nothing */
   `,
			want: nil,
		},
		{
			name: "positional-parameter-looking dollar is not a quote",
			in:   "SELECT $1 FROM t WHERE id = $2; SELECT 1;",
			want: []string{
				"SELECT $1 FROM t WHERE id = $2",
				"SELECT 1",
			},
		},
		{
			name: "semicolon inside line comment is ignored",
			in: `SELECT 1 -- ; not a terminator
;
SELECT 2;`,
			want: []string{"SELECT 1", "SELECT 2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := splitSQLStatements(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("\nin:  %q\ngot: %#v\nwant:%#v", tc.in, got, tc.want)
			}
		})
	}
}
