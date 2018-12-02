package splittar

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"time"

	"github.com/skillian/errors"
	"github.com/skillian/logging"
)

var (
	logger = logging.GetLogger("github.com/skillian/splittar")
)

// Config defines the configuration of a SplitTar operation.
type Config struct {
	block int
	source io.ReadCloser
	target *tar.Writer
}

// Option is a function that changes some option on the SplitTar configuration.
type Option func(c *Config) error

// BlockSize sets the block size to the given value.
func BlockSize(size int) Option {
	return func(c *Config) error {
		if size <= 0 {
			return errors.Errorf(
				"BlockSize cannot be <= 0")
		}
		c.block = size
		return nil
	}
}

// Source sets the source of the SplitTar operation.
func Source(r io.ReadCloser) Option {
	return func(c *Config) error {
		c.source = r
		return nil
	}
}

// SourceReader works like Source but does not close the reader after the Tar
// operation.
func SourceReader(r io.Reader) Option {
	return func(c *Config) error {
		c.source = ioutil.NopCloser(r)
		return nil
	}
}

// SourceFilename sets the source filename that's automatically opened and
// read.
func SourceFilename(name string) Option {
	return func(c *Config) (err error) {
		var f *os.File
		f, err = os.Open(name)
		if err != nil {
			return errors.ErrorfWithCause(
				err, "failed to open source file: %v", err)
		}
		c.source = f
		return nil
	}
}

// Target sets the configuration target writer.
func Target(w *tar.Writer) Option {
	return func(c *Config) error {
		c.target = w
		return nil
	}
}

// Target writer accepts an io.Writer and wraps it into a tar.Writer.
func TargetWriter(w io.Writer) Option {
	return func(c *Config) error {
		c.target = tar.NewWriter(w)
		return nil
	}
}

// NewConfig creates a new Config with the specified options.
func NewConfig(options ...Option) (*Config, error) {
	c := new(Config)
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// SplitTar reads a large file and splits it into chunks that are written
// into a single Tar archive.
func SplitTar(c *Config) (err error) {
	b := make([]byte, c.block)
	h := createTarHeader()
	defer errors.WrapDeferred(&err, c.source.Close)
	defer errors.WrapDeferred(&err, c.target.Close)
	for i := 0; ; i++ {
		var n, read int
		for read = 0; read < len(b); read += n {
			logger.Debug1("attempt reading %d bytes from source", len(b) - read)
			n, err = c.source.Read(b[read:])
			if err != nil {
				if err == io.EOF {
					break
				}
				return errors.ErrorfWithCause(
					err, "failed to read next chunk from source: %v",
					err)
			}
			logger.Debug1("actually read %d bytes from source", n)
		}
		if read == 0 {
			// if it's because the source file has been completely
			// read, don't return its EOF.
			if err == io.EOF {
				err = nil
			}
			break
		}
		name := fmt.Sprintf("%08d", i)
		h.Size = int64(read)
		h.Name = name
		logger.Debug1("writing header for %s", name)
		err = c.target.WriteHeader(&h)
		if err != nil {
			return errors.ErrorfWithCause(
				err, "failed to write header %s to tar: %v",
				name, err)
		}
		for written := 0; written < read; written += n {
			logger.Debug1("writing %d bytes to target", read - written)
			n, err = c.target.Write(b[written:])
			if err != nil {
				return errors.ErrorfWithCause(
					err, "failed to write chunk to writer: %v",
					err)
			}
		}
	}
	return
}

func createTarHeader() tar.Header {
	u, err := user.Current()
	if err != nil {
		logger.Error1("failed to get current user: %v", err)
		u = &user.User{
			Uid: "999",
			Gid: "999",
			Username: "<unknown>",
			Name: "Unknown User",
			HomeDir: "",
		}
	}
	uid, _ := strconv.Atoi(u.Uid)
	g, err := user.LookupGroupId(u.Gid)
	if err != nil {
		logger.Error2(
			"failed to look up gid: %v: %v", u.Gid, err)
		g = &user.Group{Gid: u.Gid, Name: "<unknown>"}
	}
	gid, _ := strconv.Atoi(g.Gid)
	t := time.Now()
	return tar.Header{
		Typeflag: tar.TypeReg,
		Name: "",	// set after return.
		Linkname: "",
		Size: 0,	// set after return.
		Mode: 0444,	// TODO(skillian): I don't know about this...
		Uid: uid,
		Gid: gid,
		Uname: u.Username,
		Gname: g.Name,
		ModTime: t,
		AccessTime: t,
		ChangeTime: t,
		Devmajor: 0,
		Devminor: 0,
		Xattrs: nil,
		PAXRecords: make(map[string]string),
		Format: tar.FormatPAX,
	}
}
