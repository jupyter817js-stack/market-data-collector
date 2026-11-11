package pinnacle

import "testing"

func mkMatchup(leagueName string) Matchup {
	var m Matchup
	m.League.Name = leagueName
	return m
}

func TestLeagueNamePasses(t *testing.T) {
	cases := []struct {
		name     string
		league   string
		includes []string
		excludes []string
		want     bool
	}{
		{
			name:     "empty includes + empty excludes lets everything pass",
			league:   "anything goes",
			includes: nil,
			excludes: nil,
			want:     true,
		},
		{
			name:     "include substring hit",
			league:   "italy - serie a",
			includes: []string{"italy - serie a"},
			want:     true,
		},
		{
			name:     "include substring miss",
			league:   "belarus - premier league",
			includes: []string{"italy - serie a"},
			want:     false,
		},
		{
			name:     "exclude drops women's variant of an included league",
			league:   "germany - bundesliga women",
			includes: []string{"germany - bundesliga"},
			excludes: []string{"women"},
			want:     false,
		},
		{
			name:     "exclude drops u19 variant",
			league:   "turkey - super lig u19",
			includes: []string{"turkey - super lig"},
			excludes: []string{"u19"},
			want:     false,
		},
		{
			name:     "exclude does not affect leagues that don't contain its substring",
			league:   "germany - bundesliga",
			includes: []string{"germany - bundesliga"},
			excludes: []string{"women", "u19", "u21"},
			want:     true,
		},
		{
			name:     "exclude without include allows everything except excluded",
			league:   "anything - youth league",
			includes: nil,
			excludes: []string{"youth"},
			want:     false,
		},
		{
			name:     "exclude is case-insensitive when caller lowercased input",
			league:   "england - premier league international cup",
			includes: []string{"england - premier league"},
			excludes: []string{"international cup"},
			want:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := leagueNamePasses(tc.league, tc.includes, tc.excludes)
			if got != tc.want {
				t.Fatalf("leagueNamePasses(%q, %v, %v) = %v, want %v",
					tc.league, tc.includes, tc.excludes, got, tc.want)
			}
		})
	}
}

func TestFilterMatchupsByLeagueName_AppliesIncludeAndExclude(t *testing.T) {
	matchups := []Matchup{
		mkMatchup("Germany - Bundesliga"),
		mkMatchup("Germany - Bundesliga Women"),
		mkMatchup("Italy - Serie A"),
		mkMatchup("Italy - Serie A2"), // substring of "serie a"
		mkMatchup("Belarus - Premier League"),
	}
	got := filterMatchupsByLeagueName(matchups,
		[]string{"germany - bundesliga", "italy - serie a"},
		[]string{"women", "serie a2"},
	)
	if len(got) != 2 {
		t.Fatalf("len(filtered)=%d want 2; got=%+v", len(got), got)
	}
	if got[0].League.Name != "Germany - Bundesliga" || got[1].League.Name != "Italy - Serie A" {
		t.Fatalf("unexpected survivors: %+v", got)
	}
}

func TestFilterLeagues_AppliesIncludeAndExclude(t *testing.T) {
	lgs := []League{
		{Name: "England - Premier League"},
		{Name: "England - Premier League Women"},
		{Name: "England - Premier League International Cup"},
		{Name: "Cambodia - Premier League"}, // matches include via "premier league" only — kept by design
	}
	got := filterLeagues(lgs,
		[]string{"england - premier league"},
		[]string{"women", "international cup"},
	)
	if len(got) != 1 || got[0].Name != "England - Premier League" {
		t.Fatalf("filtered=%+v want only 'England - Premier League'", got)
	}
}
