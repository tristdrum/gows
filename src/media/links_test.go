package media

import (
	"testing"
)

func TestExtractUrlFromText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Single URL",
			input:    "Check this out: https://example.com",
			expected: "https://example.com",
		},
		{
			name:     "Multiple URLs",
			input:    "First: http://google.com, Second: https://example.org",
			expected: "http://google.com", // Assuming function returns the first URL found
		},
		{
			name:     "No URL",
			input:    "This is just plain text with no links.",
			expected: "",
		},
		{
			name:     "URL with query parameters",
			input:    "Here: https://example.com?param=1&other=value",
			expected: "https://example.com?param=1&other=value",
		},
		{
			name:     "Text before and after URL",
			input:    "Start https://example.com end",
			expected: "https://example.com",
		},
		{
			name:     "FTP URL",
			input:    "Check this ftp://example.com/file.txt",
			expected: "",
		},
		{
			name:     "Malformed URL",
			input:    "This is not a link: htt://wrong.com",
			expected: "",
		},
		{
			name:     "Plain ",
			input:    "This is not a link: htt://wrong.com",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractUrlFromText(tt.input)
			if result != tt.expected {
				t.Errorf("ExtractUrlFromText(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
