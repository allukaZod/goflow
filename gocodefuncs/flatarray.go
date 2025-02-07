package gocodefuncs

import (
	"fmt"
	"github.com/LubyRuffy/goflow/utils"
	"github.com/mitchellh/mapstructure"
	"github.com/tidwall/gjson"
	"os"
	"sync/atomic"
)

type flatParams struct {
	Field string
}

func jsonArrayEnum(node gjson.Result, f func(result gjson.Result) error) error {
	if node.IsArray() {
		for _, child := range node.Array() {
			err := jsonArrayEnum(child, f)
			if err != nil {
				return err
			}
		}
	} else {
		return f(node)
	}
	return nil
}

// FlatArray 打平一个Array数据内容
func FlatArray(p Runner, params map[string]interface{}) *FuncResult {
	var err error
	var options flatParams
	if err = mapstructure.Decode(params, &options); err != nil {
		panic(err)
	}

	if len(options.Field) == 0 {
		panic(fmt.Errorf("flatArray: field cannot be empty"))
	}

	var lines int64
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

			for _, item := range gjson.Get(line, options.Field).Array() {
				err = jsonArrayEnum(item, func(result gjson.Result) error {
					if result.Str != "" {
						_, err = f.WriteString(result.String() + "\n")
					} else {
						_, err = f.WriteString(result.Raw + "\n")
					}
					return err
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		panic(fmt.Errorf("flatArray error: %w", err))
	}

	return &FuncResult{
		OutFile: fn,
	}
}
