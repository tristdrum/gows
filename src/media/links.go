package media

import (
	"context"
	"fmt"
	"github.com/devlikeapro/goscraper"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

var UrlRegex = `(http(s)?:\/\/.)(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)`
var UrlRe = regexp.MustCompile(UrlRegex)

const fetchBodyLimit = 10 * 1024 * 1024 // 10 MiB safety cap

func ExtractUrlFromText(text string) string {
	match := UrlRe.FindString(text)
	return match
}

func MakeSureURL(text string) string {
	var url string
	if !strings.HasPrefix(text, "http") && !strings.HasPrefix(text, "https") {
		url = "https://" + text
	} else {
		url = text
	}
	return url
}

type LinkPreview struct {
	Url         string
	Title       string
	Description string
	ImageUrl    string
	IconUrl     string
	Image       []byte
}

var ScrapeHeaders = map[string]string{
	"User-Agent": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
	"Accept":     "Mozilla/5.0 (Windows; Windows NT 6.3; Win64; x64) Gecko/20100101 Firefox/67.7",
}

// GoscraperFetchPreview fetches a preview of a URL using goscraper.
// https://github.com/devlikeapro/goscraper
func GoscraperFetchPreview(ctx context.Context, uri string) (*LinkPreview, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	scraper := goscraper.Scraper{
		Url:         u,
		MaxRedirect: 5,
		Headers:     ScrapeHeaders,
	}
	s, err := scraper.Scrape(ctx)
	if err != nil {
		return nil, err
	}

	var image string
	if len(s.Preview.Images) > 0 {
		image = s.Preview.Images[0]
	}
	preview := &LinkPreview{
		Url:         s.Preview.Link,
		Title:       s.Preview.Title,
		Description: s.Preview.Description,
		ImageUrl:    image,
		IconUrl:     s.Preview.Icon,
	}
	return preview, nil
}

// FetchBodyByUrl fetches the body of a given URL and returns it as a byte slice.
func FetchBodyByUrl(ctx context.Context, uri string) ([]byte, error) {
	// Create an HTTP client with a timeout
	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	headers := ScrapeHeaders
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Perform the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch URL: %w", err)
	}
	defer resp.Body.Close()

	// Check for non-2xx HTTP status codes
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	// Refuse to download bodies larger than the safety cap
	if resp.ContentLength > fetchBodyLimit {
		return nil, fmt.Errorf("HTTP body too large: %d > %d", resp.ContentLength, fetchBodyLimit)
	}

	// Read response body
	limited := io.LimitReader(resp.Body, fetchBodyLimit+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if int64(len(body)) > fetchBodyLimit {
		return nil, fmt.Errorf("HTTP body too large: > %d bytes", fetchBodyLimit)
	}

	// Return the body
	return body, nil
}
