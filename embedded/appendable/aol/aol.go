package aol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
)

const (
	// privateFileMode grants owner to read/write a file.
	privateFileMode = 0600
)

var (
	ErrCorrupt  = errors.New("log corrupt")
	ErrClosed   = errors.New("log closed")
	ErrNotFound = errors.New("not found")
	ErrEOF      = errors.New("end of file reached")
)

var _ appendable.Appendable = &Log{}

type Options struct {
	// DisableSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	DisableSync bool
	SegmentSize int    // SegmentSize of each segment. Default is 20 MB.
	ReadOnly    bool   // Open file(s) in read-only mode
	FileExt     string // File extension of each log file
	DirPerms    os.FileMode
	FilePerms   os.FileMode
}

func (o *Options) validate() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultOptions.SegmentSize
	}

	if o.DirPerms == 0 {
		o.DirPerms = DefaultOptions.DirPerms
	}

	if o.FilePerms == 0 {
		o.FilePerms = DefaultOptions.FilePerms
	}

	if o.FileExt == "" {
		o.FileExt = DefaultOptions.FileExt
	}
}

var DefaultOptions = &Options{
	DisableSync: false,    // Fsync after every write
	SegmentSize: 20971520, // 20 MB log segment files.
	DirPerms:    0750,
	FilePerms:   0640,
	FileExt:     "aof",
}

// Log represents a append only log
type Log struct {
	mu       sync.RWMutex
	path     string     // absolute path to log directory
	segments []*segment // all known log segments
	sfile    *os.File   // tail segment file handle
	wbatch   Batch      // reusable write batch

	opts    Options
	closed  bool
	corrupt bool
}

// segment represents a single segment file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	cbuf  []byte // cached entries buffer
	cpos  []bpos // cached entries positions in buffer
}

type bpos struct {
	pos int // byte position
	end int // one byte past pos
}

func Open(path string, opts *Options) (*Log, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	opts.validate()

	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, opts: *opts}
	if err := os.MkdirAll(path, l.opts.DirPerms); err != nil {
		return nil, err
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) load() error {
	files, err := ioutil.ReadDir(l.path)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()

		if file.IsDir() || len(name) < 8 {
			continue
		}

		index, err := strconv.ParseUint(name[:8], 10, 64)
		if err != nil || index == 0 {
			continue
		}

		if len(name) == 8 {
			l.segments = append(l.segments, &segment{
				index: index,
				path:  filepath.Join(l.path, name),
			})
		}
	}

	if len(l.segments) == 0 {
		// Create a new log
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1, l.opts.FileExt)),
		})
		l.sfile, err = os.OpenFile(l.segments[0].path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		return err
	}

	// Open the last segment for appending
	lseg := l.segments[len(l.segments)-1]
	if l.opts.ReadOnly {
		l.sfile, err = os.OpenFile(l.path, os.O_RDWR, privateFileMode)
		if err != nil {
			return err
		}
	} else {
		l.sfile, err = os.OpenFile(lseg.path, os.O_WRONLY, l.opts.FilePerms)
		return err
	}

	if _, err := l.sfile.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// Load the last segment entries
	if err := l.loadSegmentEntries(lseg); err != nil {
		return err
	}

	return nil
}

func segmentName(index uint64, ext string) string {
	return fmt.Sprintf("%08d.%s", index, ext)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		if l.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	l.closed = true
	if l.corrupt {
		return ErrCorrupt
	}
	return nil
}

func (l *Log) Write(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	l.wbatch.Clear()
	l.wbatch.Write(data)
	return l.writeBatch(&l.wbatch)
}

func (l *Log) appendEntry(dst []byte, data []byte) (out []byte, cpos bpos) {
	return appendBinaryEntry(dst, data)
}

func (l *Log) cycle() error {
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}

	nidx := l.segments[len(l.segments)-1].index + 1
	s := &segment{
		index: nidx,
		path:  filepath.Join(l.path, segmentName(nidx, l.opts.FileExt)),
	}
	var err error
	l.sfile, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	return nil
}

func appendBinaryEntry(dst []byte, data []byte) (out []byte, cpos bpos) {
	// data_size + data
	pos := len(dst)
	dst = appendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	return dst, bpos{pos, len(dst)}
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	size int
}

func (b *Batch) Write(data []byte) {
	b.entries = append(b.entries, batchEntry{len(data)})
	b.datas = append(b.datas, data...)
}

func (b *Batch) Clear() {
	b.entries = b.entries[:0]
	b.datas = b.datas[:0]
}

func (l *Log) WriteBatch(b *Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	if len(b.datas) == 0 {
		return nil
	}
	return l.writeBatch(b)
}

func (l *Log) writeBatch(b *Batch) error {
	// load the tail segment
	s := l.segments[len(l.segments)-1]
	if len(s.cbuf) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return err
		}
		s = l.segments[len(l.segments)-1]
	}

	mark := len(s.cbuf)
	datas := b.datas
	for i := 0; i < len(b.entries); i++ {
		data := datas[:b.entries[i].size]
		var cpos bpos
		s.cbuf, cpos = l.appendEntry(s.cbuf, data)
		s.cpos = append(s.cpos, cpos)
		if len(s.cbuf) >= l.opts.SegmentSize {
			// segment has reached capacity, cycle now
			if _, err := l.sfile.Write(s.cbuf[mark:]); err != nil {
				return err
			}
			if err := l.cycle(); err != nil {
				return err
			}
			s = l.segments[len(l.segments)-1]
			mark = 0
		}
		datas = datas[b.entries[i].size:]
	}
	if len(s.cbuf)-mark > 0 {
		if _, err := l.sfile.Write(s.cbuf[mark:]); err != nil {
			return err
		}
	}

	if !l.opts.DisableSync {
		if err := l.sfile.Sync(); err != nil {
			return err
		}
	}

	b.Clear()
	return nil
}

// findSegment performs a bsearch on the segments
func (l *Log) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func (l *Log) loadSegmentEntries(s *segment) error {
	data, err := ioutil.ReadFile(s.path)
	if err != nil {
		return err
	}
	ebuf := data
	var cpos []bpos
	var pos int
	for len(data) > 0 {
		var n int
		n, err = loadEntry(data)
		if err != nil {
			return err
		}
		data = data[n:]
		cpos = append(cpos, bpos{pos, pos + n})
		pos += n
	}
	s.cbuf = ebuf
	s.cpos = cpos
	return nil
}

func loadEntry(data []byte) (n int, err error) {
	// data_size + data
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, ErrCorrupt
	}
	return n + int(size), nil
}

func (l *Log) loadSegment(index uint64) (*segment, error) {
	// check the last segment first.
	lseg := l.segments[len(l.segments)-1]
	if index >= lseg.index {
		return lseg, nil
	}

	// find in the segment array
	idx := l.findSegment(index)
	s := l.segments[idx]
	if len(s.cpos) == 0 {
		// load the entries from cache
		if err := l.loadSegmentEntries(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (l *Log) Read(segment, index uint64) (data []byte, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return nil, ErrCorrupt
	} else if l.closed {
		return nil, ErrClosed
	}
	if segment == 0 {
		return nil, ErrNotFound
	}
	s, err := l.loadSegment(segment)
	if err != nil {
		return nil, err
	}

	if int(index) >= len(s.cpos) {
		return nil, ErrEOF
	}
	cpos := s.cpos[index]
	edata := s.cbuf[cpos.pos:cpos.end]
	// binary read
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, ErrCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, ErrCorrupt
	}
	data = make([]byte, size)
	copy(data, edata[n:])
	return data, nil
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.sfile.Sync()
}

func (l *Log) Flush() error {
	return l.Sync()
}

func (l *Log) Offset() int64 {
	return l.Sync()
}
