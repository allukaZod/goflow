package gocodefuncs

import (
	"errors"
	"github.com/LubyRuffy/goflow/utils"
	"github.com/itchyny/gojq/cli"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"os"
)

type jqQueryParams struct {
	Query  string `json:"query"`
	Stream bool
}

func doJqQuery(inFile *os.File, options jqQueryParams, onData func([]byte)) error {
	args := []string{os.Args[0]}
	if options.Stream {
		args = append(args, "-s")
	}
	origArgs := os.Args
	defer func() {
		os.Args = origArgs
	}()
	os.Args = append(args, "-c", options.Query)

	outR, outW, _ := os.Pipe()
	defer func() {
		outR.Close()
		outW.Close()
	}()
	origStdout := os.Stdout
	defer func() {
		os.Stdout = origStdout
	}()
	os.Stdout = outW

	origStdin := os.Stdin
	defer func() {
		os.Stdin = origStdin
	}()
	os.Stdin = inFile

	errR, errW, _ := os.Pipe()
	defer func() {
		errR.Close()
		errW.Close()
	}()
	os.Stderr = errW

	status := cli.Run()
	errW.Close()
	outW.Close()
	if status == 0 {
		buf, err := ioutil.ReadAll(outR)
		if err != nil {
			panic(err)
		}

		onData(buf)
	} else {
		buf, err := ioutil.ReadAll(errR)
		if err != nil {
			panic(err)
		}
		//log.Println(string(buf[:n]))
		panic(errors.New(string(buf)))
	}

	return nil
}

// JqQuery jq command
func JqQuery(p Runner, params map[string]interface{}) *FuncResult {
	var fn string
	var err error
	var options jqQueryParams
	if err = mapstructure.Decode(params, &options); err != nil {
		panic(err)
	}

	inFile, err := os.Open(p.GetLastFile())
	if err != nil {
		panic(err)
	}
	defer inFile.Close()

	err = doJqQuery(inFile, options, func(buf []byte) {
		fn, err = utils.WriteTempFile(".json", func(f *os.File) error {
			_, err = f.Write(buf)
			if err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		panic(err)
	}

	return &FuncResult{
		OutFile: fn,
	}
}
