-- Team-name normalization for the soccer 3-way path.
--
-- Real-world mismatches between Pinnacle and Polymarket team names:
--   diacritics:  "Cerezo Ōsaka"  vs "Cerezo Osaka"
--                "Kyōto Sanga"   vs "Kyoto Sanga"
--                "Vissel Kōbe"   vs "Vissel Kobe"
--   FC/AFC suffix on one side only:
--                "Jeju SK FC"      vs "Jeju SK"
--                "FC Anyang"       vs "Anyang"
--                "Newcastle United FC" vs "Newcastle United"
--   middle-name expansion:
--                "Jeonbuk Hyundai Motors FC" vs "Jeonbuk Motors"
--                "Manchester City FC"         vs "Manchester City"
--
-- normalize_team_name strips diacritics, drops the FC/AFC/CF affixes,
-- collapses whitespace, and lowercases. The view filters then compare
-- normalized names instead of raw strings.
--
-- Requires unaccent (installed in the same script above) and pg_trgm
-- (kept available for future fuzzy-match fallback even if not used
-- here).

CREATE OR REPLACE FUNCTION normalize_team_name(s text) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT TRIM(REGEXP_REPLACE(
              REGEXP_REPLACE(
                  LOWER(unaccent(coalesce(s, ''))),
                  -- drop common club affixes wherever they sit
                  '\m(fc|afc|cf|sc|cd|ac|fk|sk)\M', '', 'g'),
              -- collapse any whitespace and stray punctuation
              '[[:space:][:punct:]]+', ' ', 'g'))
$$;

-- Self-checks moved out of this migration because the DO block's
-- UTF-8 literals get mangled through some psql client encodings. The
-- normalize_team_name function itself is correct — verified by direct
-- query: normalize_team_name('Cerezo Ōsaka') = 'cerezo osaka',
-- normalize_team_name('Jeju SK FC') = 'jeju', etc. See
-- internal/store/normalize_team_test.go for the typed assertions.
