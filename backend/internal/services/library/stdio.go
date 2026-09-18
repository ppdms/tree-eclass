package library

import (
	"errors"
	"io"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The protocol uses one JSON message per line. Bound each inbound frame even
// when a local client never terminates it, without buffering an entire frame.
type frameReader struct {
	io.ReadCloser
	size int
}

func (r *frameReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	for _, b := range p[:n] {
		if b == '\n' {
			r.size = 0
		} else {
			r.size++
			if r.size > 128*1024 {
				return 0, errors.New("MCP input frame exceeds 128 KiB")
			}
		}
	}
	return n, err
}
func Stdio(input io.ReadCloser, output io.WriteCloser) mcp.Transport {
	return &mcp.IOTransport{Reader: &frameReader{ReadCloser: input}, Writer: output}
}
