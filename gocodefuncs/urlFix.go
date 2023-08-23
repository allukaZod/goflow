package gocodefuncs

import (
	"fmt"
	"github.com/LubyRuffy/goflow/utils"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"os"
	"sync/atomic"
)

// UrlFix 自动补齐url
func UrlFix(p Runner, params map[string]interface{}) *FuncResult {
	var fn string
	var err error
	field := "url"
	if len(params) > 0 {
		field = params["urlField"].(string)
	}
	if len(field) == 0 {
		panic(fmt.Errorf("urlFix must has a field"))
	}

	var lines int64
	if lines, err = utils.FileLines(p.GetLastFile()); err != nil {
		panic(fmt.Errorf("ParseURL error: %w", err))
	}
	if lines == 0 {
		return &FuncResult{}
	}
	var processed int64

	fn, err = utils.WriteTempFile(".json", func(f *os.File) error {
		return utils.EachLineWithContext(p.GetContext(), p.GetLastFile(), func(line string) error {
			defer func() {
				atomic.AddInt64(&processed, 1)
				p.SetProgress(float64(processed) / float64(lines))
			}()

			v := gjson.Get(line, field).String()
			if len(v) == 0 {
				// 没有字段，直接写回原始行
				_, err = f.WriteString(line + "\n")
				return err
			}

			line, err = sjson.Set(line, field, utils.FixURL(v))
			if err != nil {
				return err
			}
			_, err = f.WriteString(line + "\n")
			return err
		})
	})
	if err != nil {
		panic(fmt.Errorf("urlFix failed: %w", err))
	}

	return &FuncResult{
		OutFile: fn,
	}
}
