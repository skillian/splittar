package main

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/skillian/argparse"
	"github.com/skillian/errors"
	"github.com/skillian/util"
)

const (
	description = `Split a source file (or standard input if no source is
provided) into SIZE chunks and write them to an output tar file (or standard
output if no output file is specified).

This command is intended to be used as a preprocessor to pipe tar archives to
other commands.
`
)

var (
	currentUser = mustGetCurrentUser()
	uid = mustParseInt(currentUser.Uid)
	gid = mustParseInt(currentUser.Gid)

	parser = argparse.MustNewArgumentParser(
		argparse.Description(description),
	)

	suffixLengthArg = parser.MustAddArgument(
		argparse.OptionStrings("-a", "--suffix-length"),
		argparse.Action("store"),
		argparse.Type(argparse.Int),
		argparse.Help("generate suffixes of length N (default 2)"),
		argparse.Default(2),
	)

	sizeArg = parser.MustAddArgument(
		argparse.OptionStrings("-b", "--bytes"),
		argparse.Action("store"),
		argparse.MetaVar("SIZE"),
		argparse.Type(argparse.String),
		argparse.Help("put SIZE bytes per output file"),
	)

	srcFileArg = parser.MustAddArgument(
		argparse.Dest("file"),
		argparse.Action("store"),
		argparse.MetaVar("FILE"),
		argparse.Default("-"),
		argparse.Help("Source file to split"),
	)

	tarFileArg = parser.MustAddArgument(
		argparse.Dest("target"),
		argparse.Action("store"),
		argparse.MetaVar("TAR_FILE"),
		argparse.Default("-"),
		argparse.Help("Target tar file to write to"),
	)

	ns = parser.MustParseArgs()

	suffixLength = ns.MustGet(suffixLengthArg).(int)
	sizeString = ns.MustGet(sizeArg).(string)
	srcFileName = ns.MustGet(srcFileArg).(string)
	tarFileName = ns.MustGet(tarFileArg).(string)
)

func main() {
	size, err := getSize(sizeString)
	exitOnError(err)

	source := os.Stdin
	if srcFileName != "" && srcFileName != "-" {
		source, err = os.Open(srcFileName)
		exitOnError(err)
		defer source.Close()
	}

	baseName := filepath.Base(srcFileName)

	if source != os.Stdin {
		fi, err := source.Stat()
		exitOnError(err)
		fs := fi.Size()
		if fs < size {
			size = fs
		}
	}

	target := os.Stdout
	if tarFileName != "" && tarFileName != "-" {
		target, err = os.Create(tarFileName)
		exitOnError(err)
		defer target.Close()
	}

	tarFile := tar.NewWriter(target)
	defer tarFile.Close()

	b := make([]byte, util.Int64Min(size, 32768))

	now := time.Now()

	h := tar.Header{
		Typeflag: tar.TypeReg,
		//Name tbd
		//Size tbd
		Mode: 0400,
		Uid: uid,
		Gid: gid,
		Uname: currentUser.Username,
		Gname: currentUser.Username,	// TODO: who cares?
		ModTime: now,
		AccessTime: now,
		ChangeTime: now,
		Format: tar.FormatGNU,
	}

	padFmt := fmt.Sprintf("%%0%dd", suffixLength)

	readfile:
	for i := 0; ; i++ {
		c := 0
		readblock:
		for c < len(b) {
			n, err := source.Read(b[c:])
			c += n
			if err == io.EOF {
				break readblock
			}
			exitOnError(err)
		}
		if c == 0 {
			break readfile
		}
		h.Name = strings.Join([]string{
			baseName,
			fmt.Sprintf(padFmt, i),
		}, ".")
		h.Size = int64(c)
		err = tarFile.WriteHeader(&h)
		exitOnError(err)
		w, err := tarFile.Write(b)
		exitOnError(err)
		if w != c {
			fmt.Fprintf(
				os.Stderr, "bytes written to tar (%d) does "+
				"not equal expected count (%d)\n",
				w, c,
			)
			// TODO: should this be fatal?
		}
	}
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}

var (
	errEmptySize = errors.New("empty size")
)

func getSize(v string) (int64, error) {
	if len(v) == 0 {
		return 0, errEmptySize
	}
	suffix := v[len(v)-1]
	mult := int64(1)
	if !isdigit(suffix) {
		v = v[:len(v)-1]
		var ok bool
		if mult, ok = sizeMults[suffix]; !ok {
			return 0, errors.Errorf(
				"invalid size multiplier: %v", suffix,
			)
		}
	}
	size, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, errors.ErrorfWithCause(
			err, "invalid size",
		)
	}
	return size * mult, nil
}

func isdigit(b byte) bool {
	return '0' <= b && b <= '9'
}

func mustGetCurrentUser() *user.User {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return u
}

func mustParseInt(v string) int {
	i, err := strconv.Atoi(v)
	if err != nil {
		panic(err)
	}
	return i
}

var sizeMults = map[byte]int64{
	'b': 1,
	'B': 1,
	'k': 1000,
	'K': 1024,
	'm': 1000 * 1000,
	'M': 1024 * 1024,
	'g': 1000 * 1000 * 1000,
	'G': 1024 * 1024 * 1024,
	't': 1000 * 1000 * 1000 * 1000,
	'T': 1024 * 1024 * 1024 * 1024,
	'p': 1000 * 1000 * 1000 * 1000,
	'P': 1024 * 1024 * 1024 * 1024,
}

