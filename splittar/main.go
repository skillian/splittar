package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/skillian/errors"
	"github.com/skillian/logging"
	"github.com/skillian/splittar"
)

var (
	logger = logging.GetLogger("github.com/skillian/splittar")
)

func init() {
	h := new(logging.ConsoleHandler)
	h.SetLevel(logging.DebugLevel)
	h.SetFormatter(logging.DefaultFormatter{})
	logger.AddHandler(h)
}

func usage() {
	fmt.Printf(`usage:
    %s [ -h ] [ -b BLOCK_SIZE ] ( SOURCE_FILE | - ) ( TARGET_FILE | - )

Positional arguments:

    SOURCE_FILE:    The source file that is read and segmented into smaller
                    files within the output tar.

    TARGET_FILE:    The target tar file.

Optional arguments:

    -b      The max block size of each file within the tar.
    -h      This help documentation

`, path.Base(os.Args[0]))
}

func main() {
	block := 0
	flag.IntVar(&block, "b", 1 << 26, "")

	levelString := ""
	flag.StringVar(&levelString, "l", "Warn", "")

	flag.Usage = usage

	flag.Parse()

	if lvl, ok := logging.ParseLevel(levelString); ok {
		logger.SetLevel(lvl)
	}

	positionals := flag.Args()

	if len(positionals) != 2 {
		die(errors.Errorf(
			"exactly 2 positional arguments required"))
	}

	if err := splitTar(positionals[0], positionals[1], block); err != nil {
		die(err)
	}
}

func splitTar(sourceName, targetName string, block int) (err error) {
	source, err := getSourceFile(sourceName, ioutil.NopCloser(os.Stdin))
	if err != nil {
		die(errors.ErrorfWithCause(
			err, "failed to get source file: %v", err))
	}

	target, err := getTargetFile(targetName, nopWriteCloser{os.Stdout})
	if err != nil {
		die(errors.ErrorfWithCause(
			err, "failed to get target file: %v", err))
	}

	c, err := splittar.NewConfig(
		splittar.BlockSize(block),
		splittar.Source(source),
		splittar.TargetWriter(target))

	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to initialize splittar config: %v", err)
	}

	return splittar.SplitTar(c)
}

// die prints an error message to stdout and ends the program with a non-zero
// return code.
func die(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(-1)
}

func getSourceFile(name string, def io.ReadCloser) (io.ReadCloser, error) {
	if name == "" || name == "-" {
		return def, nil
	}
	return os.Open(name)
}

func getTargetFile(name string, def io.WriteCloser) (io.WriteCloser, error) {
	if name == "" || name == "-" {
		return def, nil
	}
	return os.Create(name)
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
