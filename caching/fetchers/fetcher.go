package fetchers

import "io"

type Fetcher interface {
	Fetch(namespace string, key string) (io.ReadCloser, error)
}
