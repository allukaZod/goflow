package gocodefuncs

import (
	"fmt"
	"github.com/LubyRuffy/goflow/utils"
	"github.com/tidwall/sjson"
	"os"
	"strings"
	"sync/atomic"
)

// RemoveField 移除字段
func RemoveField(p Runner, params map[string]interface{}) *FuncResult {
	if len(p.GetLastFile()) == 0 {
		panic(fmt.Errorf("removeField need input pipe"))
	}

	fields := strings.Split(params["fields"].(string), ",")

	var lines int64
	var err error
	if lines, err = utils.FileLines(p.GetLastFile()); err != nil {
		panic(fmt.Errorf("ParseURL error: %w", err))
	}
	if lines == 0 {
		return &FuncResult{}
	}
	var processed int64

	var fn string

	fn, err = utils.WriteTempFile(".json", func(f *os.File) error {
		return utils.EachLineWithContext(p.GetContext(), p.GetLastFile(), func(line string) error {
			defer func() {
				atomic.AddInt64(&processed, 1)
				p.SetProgress(float64(processed) / float64(lines))
			}()

			var err error
			newLine := line
			for _, field := range fields {
				newLine, err = sjson.Delete(newLine, field)
				if err != nil {
					return err
				}
			}
			_, err = f.WriteString(newLine + "\n")
			if err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		panic(fmt.Errorf("removeField error: %w", err))
	}

	return &FuncResult{
		OutFile: fn,
	}
}
