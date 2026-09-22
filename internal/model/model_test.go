package model

import "testing"

func TestFilterMatch(t *testing.T) {
	r := &Record{ID: "x", Meta: map[string]string{"g": "doc", "lang": "zh"}}
	cases := []struct {
		name string
		f    Filter
		want bool
	}{
		{"empty matches", Filter{}, true},
		{"single eq hit", Filter{Eq: map[string]string{"g": "doc"}}, true},
		{"both eq hit", Filter{Eq: map[string]string{"g": "doc", "lang": "zh"}}, true},
		{"value mismatch", Filter{Eq: map[string]string{"g": "img"}}, false},
		{"missing key", Filter{Eq: map[string]string{"x": "y"}}, false},
		{"one of two fails", Filter{Eq: map[string]string{"g": "doc", "lang": "en"}}, false},
	}
	for _, c := range cases {
		if got := c.f.Match(r); got != c.want {
			t.Fatalf("%s: Match=%v want %v", c.name, got, c.want)
		}
	}
	t.Log("JUDGEMENT filter = AND of equality constraints over authoritative metadata")
}
