package wal

import "io"

var defaultBufferBytes = 128 * 1024

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
type PageWriter struct {
	w io.Writer
	// pageOffset tracks the page offset of the base of the buffer
	pageOffset int
	// pageBytes is the number of bytes per page
	pageBytes int
	// bufferedBytes counts the number of bytes pending for write in the buffer
	bufferedBytes int
	// buf holds the write buffer
	buf []byte
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter. pageBytes is the number of bytes
// to write per page. pageOffset is the starting offset of io.Writer.
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,
		buf:               make([]byte, defaultBufferBytes+pageBytes),
		bufWatermarkBytes: defaultBufferBytes,
	}
}

func (pw *PageWriter) Write(p []byte) (n int, err error) {
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// no overflow
		copy(pw.buf[pw.bufferedBytes:], p)
		pw.bufferedBytes += len(p)
		return len(p), nil
	}
	// complete the slack page in the buffer if unaligned
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	if slack != pw.pageBytes {
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			slack = len(p)
		}
		// special case: writing to slack page in buffer
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]
		if partial {
			// avoid forcing an unaligned flush
			return n, nil
		}
	}
	// buffer contents are now page-aligned; clear out
	if err = pw.Flush(); err != nil {
		return n, err
	}
	// directly write all complete pages without copying
	if len(p) > pw.pageBytes {
		pages := len(p) / pw.pageBytes
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}
		p = p[pages*pw.pageBytes:]
	}
	// write remaining tail to buffer
	c, werr := pw.Write(p)
	n += c
	return n, werr
}

// Flush flushes buffered data.
func (pw *PageWriter) Flush() error {
	_, err := pw.flush()
	return err
}

func (pw *PageWriter) flush() (int, error) {
	if pw.bufferedBytes == 0 {
		return 0, nil
	}
	n, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return n, err
}
