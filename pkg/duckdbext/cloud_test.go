package duckdbext

import (
	"regexp"
	"testing"
)

func TestParseGCSURL(t *testing.T) {
	tests := []struct {
		input      string
		wantBucket string
		wantPath   string
		wantErr    bool
	}{
		{"gs://my-bucket/data/events.parquet", "my-bucket", "data/events.parquet", false},
		{"gs://bucket/file.csv", "bucket", "file.csv", false},
		{"gcs://other-bucket/path/to/file.json", "other-bucket", "path/to/file.json", false},
		{"gs://bucket/*.parquet", "bucket", "*.parquet", false},
		{"gs://bucket/prefix/**/*.parquet", "bucket", "prefix/**/*.parquet", false},
		{"gs://bucket-only", "", "", true}, // no object path
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			bucket, path, err := parseGCSURL(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseGCSURL(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseGCSURL(%q) unexpected error: %v", tt.input, err)
				return
			}
			if bucket != tt.wantBucket {
				t.Errorf("parseGCSURL(%q) bucket = %q, want %q", tt.input, bucket, tt.wantBucket)
			}
			if path != tt.wantPath {
				t.Errorf("parseGCSURL(%q) path = %q, want %q", tt.input, path, tt.wantPath)
			}
		})
	}
}

func TestGlobToRegex(t *testing.T) {
	tests := []struct {
		glob  string
		input string
		match bool
	}{
		// Simple wildcard
		{"data/*.parquet", "data/file.parquet", true},
		{"data/*.parquet", "data/other.parquet", true},
		{"data/*.parquet", "data/sub/file.parquet", false}, // * doesn't cross /
		{"data/*.parquet", "data/file.csv", false},

		// Double wildcard
		{"data/**/*.parquet", "data/a/b/file.parquet", true},
		{"data/**/*.parquet", "data/file.parquet", true},

		// Question mark
		{"data/file?.csv", "data/file1.csv", true},
		{"data/file?.csv", "data/fileA.csv", true},
		{"data/file?.csv", "data/file12.csv", false},

		// Dots in path (should be literal)
		{"data/v1.0/*.json", "data/v1.0/events.json", true},
		{"data/v1.0/*.json", "data/v1X0/events.json", false},

		// No glob characters
		{"data/exact.parquet", "data/exact.parquet", true},
		{"data/exact.parquet", "data/other.parquet", false},
	}

	for _, tt := range tests {
		t.Run(tt.glob+"_"+tt.input, func(t *testing.T) {
			pattern := globToRegex(tt.glob)
			re, err := regexp.Compile("^" + pattern + "$")
			if err != nil {
				t.Fatalf("globToRegex(%q) produced invalid regex %q: %v", tt.glob, pattern, err)
			}
			matched := re.MatchString(tt.input)
			if matched != tt.match {
				t.Errorf("globToRegex(%q) against %q: got %v, want %v (regex: %s)",
					tt.glob, tt.input, matched, tt.match, pattern)
			}
		})
	}
}

func TestNeedsGCSSignedURLs(t *testing.T) {
	tests := []struct {
		name  string
		query string
		cc    CloudConfig
		want  bool
	}{
		{
			name:  "gs:// without service account key",
			query: "SELECT * FROM read_parquet('gs://bucket/data/*.parquet')",
			cc:    CloudConfig{},
			want:  true,
		},
		{
			name:  "gs:// with service account key",
			query: "SELECT * FROM read_parquet('gs://bucket/data/*.parquet')",
			cc:    CloudConfig{GCSServiceAccountKey: "/path/to/key.json"},
			want:  false,
		},
		{
			name:  "s3:// path (not GCS)",
			query: "SELECT * FROM read_parquet('s3://bucket/data/*.parquet')",
			cc:    CloudConfig{},
			want:  false,
		},
		{
			name:  "no cloud paths",
			query: "SELECT * FROM events WHERE id > 100",
			cc:    CloudConfig{},
			want:  false,
		},
		{
			name:  "gcs:// alternative prefix",
			query: "SELECT * FROM read_csv('gcs://bucket/file.csv')",
			cc:    CloudConfig{},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NeedsGCSSignedURLs(tt.query, tt.cc)
			if got != tt.want {
				t.Errorf("NeedsGCSSignedURLs(%q, ...) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestGCSPathRegex(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "single gs:// path",
			query: "SELECT * FROM read_parquet('gs://bucket/data/*.parquet')",
			want:  []string{"gs://bucket/data/*.parquet"},
		},
		{
			name:  "multiple gs:// paths",
			query: "SELECT * FROM read_parquet('gs://b1/a.parquet') UNION ALL SELECT * FROM read_parquet('gs://b2/b.parquet')",
			want:  []string{"gs://b1/a.parquet", "gs://b2/b.parquet"},
		},
		{
			name:  "no gs:// paths",
			query: "SELECT * FROM events WHERE id > 100",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := gcsPathRe.FindAllStringSubmatch(tt.query, -1)
			var got []string
			for _, m := range matches {
				got = append(got, m[1])
			}
			if len(got) != len(tt.want) {
				t.Errorf("gcsPathRe on %q: got %v, want %v", tt.query, got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("gcsPathRe match[%d]: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
