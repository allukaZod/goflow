package gocodefuncs

import (
	"context"
	"fmt"
	"github.com/LubyRuffy/goflow/utils"
	"github.com/mitchellh/mapstructure"
	"github.com/tidwall/gjson"
	"github.com/xuri/excelize/v2"
	"log"
	"strings"
	"sync/atomic"
)

type toExcelParam struct {
	RawFormat  bool `mapstructure:"rawFormat"`  // 是否原始格式 {"Sheet1":[[]], "Sheet2":[[]]}
	InsertPic  bool `mapstructure:"insertPic"`  // 是否将截图字段自动替换为图片, rawFormat不受该参数影响
	JsonFormat bool `mapstructure:"jsonFormat"` // 从 json 直接格式化为 excel
}

// ToExcel 写excel文件
func ToExcel(p Runner, params map[string]interface{}) *FuncResult {
	var fn string
	var err error

	var options toExcelParam
	if err = mapstructure.Decode(params, &options); err != nil {
		panic(fmt.Errorf("screenShot failed: %w", err))
	}

	fn, err = utils.WriteTempFile(".xlsx", nil)
	if err != nil {
		panic(fmt.Errorf("toExcel failed: %w", err))
	}

	f := excelize.NewFile()
	defer f.Close()

	var formattedFile string
	if options.InsertPic {
		formattedFile = p.GetLastFile()
	} else {
		// 格式化资源字段
		formattedFile, err = p.FormatResourceFieldInJson(p.GetLastFile())
		if err != nil {
			panic(fmt.Errorf("format resource field in json failed: %w", err))
		}
	}

	var lines int64
	if lines, err = utils.FileLines(p.GetLastFile()); err != nil {
		panic(fmt.Errorf("ToExcel error: %w", err))
	}
	if lines == 0 {
		return &FuncResult{}
	}

	// 创建空白单元格样式
	style, err := f.NewStyle(&excelize.Style{
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
			WrapText:   true,
		},
	})
	if err != nil {
		fmt.Println(err)
	}

	var streamWriter *excelize.StreamWriter
	if !options.RawFormat && !options.JsonFormat {
		streamWriter, err = f.NewStreamWriter("Sheet1")
		if err != nil {
			panic(err)
		}
	}

	lineNum := 0
	var processed int64
	err = utils.EachLineWithContext(context.TODO(), formattedFile, func(line string) error {
		defer func() {
			atomic.AddInt64(&processed, 1)
			p.SetProgress(float64(processed) / float64(lines))
		}()
		lineNum++
		if options.JsonFormat {
			err = jsonFormatToExcel(f, line, lineNum)
			if err != nil {
				return err
			}
		} else if options.RawFormat {
			v := gjson.ParseBytes([]byte(line))
			colNo := 'A'
			v.ForEach(func(key, value gjson.Result) bool {
				// 合并的记录数据
				if strings.HasPrefix(key.String(), "_merged_") {
					return true
				}

				index := f.NewSheet(key.String())
				f.SetActiveSheet(index)
				for rows := range value.Array() {
					for cols := range value.Array()[rows].Array() {
						// 格式化单元格
						err = f.SetColWidth(key.String(), fmt.Sprintf("%c", colNo+int32(cols)),
							fmt.Sprintf("%c", colNo+int32(cols)), 30)
						if err != nil {
							panic(fmt.Errorf("ToExcel SetColWidth failed: %w", err))
						}
						err = f.SetRowHeight(key.String(), rows+1, 35)
						if err != nil {
							panic(fmt.Errorf("ToExcel SetRowHeight failed: %w", err))
						}
						err = f.SetCellStyle(key.String(), fmt.Sprintf("%c%d", colNo+int32(cols), rows+1),
							fmt.Sprintf("%c%d", colNo+int32(cols), rows+1), style)
						if err != nil {
							panic(fmt.Errorf("ToExcel SetCellStyle failed: %w", err))
						}
						// 写入内容
						err = f.SetCellValue(key.String(), fmt.Sprintf("%c%d", colNo+int32(cols), rows+1),
							value.Array()[rows].Array()[cols].Value())
						if err != nil {
							panic(fmt.Errorf("ToExcel SetCellValue failed: %w", err))
						}
					}
				}

				// 单元格合并，格式 "_merged_Sheet2":[["A2:B3","dct11"]]}
				/**
				当 key 中存在 "." 时，直接使用 v.Get 会被解析为多层嵌套 json，需要用 v.Map() 直接指定
				*/
				sheetName := key.String()
				if mergedCells, ok := v.Map()["_merged_"+sheetName]; ok && mergedCells.Exists() && mergedCells.IsArray() {
					for _, c := range mergedCells.Array() {
						err = f.MergeCell(key.String(), c.Array()[0].String(), c.Array()[1].String())
						if err != nil {
							panic(fmt.Errorf("MergeCell failed: %w", err))
						}
					}
				}
				return true
			})
		} else {

			lineNo := 2
			err = utils.EachLineWithContext(p.GetContext(), formattedFile, func(line string) error {
				v := gjson.Parse(line)
				colNo := 'A'

				v.ForEach(func(key, value gjson.Result) bool {

					// 设置第一行
					//if lineNo == 2 {
					//	err = f.SetCellValue("Sheet1", fmt.Sprintf("%c%d", colNo, lineNo-1), key.Value())
					//}
					if lineNo == 2 {
						if err = streamWriter.SetRow(fmt.Sprintf("%c%d", colNo, lineNo-1),
							[]interface{}{excelize.Cell{Value: key.Value()}},
							excelize.RowOpts{Height: 45, Hidden: false}); err != nil {
							log.Println(err)
						}
					}

					// 如果配置了写入图片项，直接设置图片
					if flds, ok := p.GetObject(utils.ResourceFieldsObjectName); ok && options.InsertPic {
						// 逐行进行文件名替换
						var file string
						for _, fld := range flds.([]string) {
							if fld == key.String() {
								file = gjson.Get(line, key.String()).String()
								err = f.AddPicture("Sheet1", fmt.Sprintf("%c%d", colNo, lineNo), file,
									`{"autofit": true}`)
								colNo++
								if err != nil {
									return false
								}
								// 完成，这个框里不写文字了
								return true
							}
						}
					}

					// 写值
					//err = f.SetCellValue("Sheet1", fmt.Sprintf("%c%d", colNo, lineNo), value.Value())
					if err = streamWriter.SetRow(fmt.Sprintf("%c%d", colNo, lineNo),
						[]interface{}{excelize.Cell{Value: value.Value()}},
						excelize.RowOpts{Height: 45, Hidden: false}); err != nil {
						panic(fmt.Errorf("SetCellValue failed: %w", err))
					}

					colNo++
					//if err != nil {
					//	panic(fmt.Errorf("SetCellValue failed: %w", err))
					//}
					return true

				})
				lineNo++
				return err
			})
			if err != nil {
				panic(fmt.Errorf("toExcel failed: %w", err))
			}

		}

		return nil
	})
	if err != nil {
		return nil
	}
	if !options.RawFormat && !options.JsonFormat {
		streamWriter.Flush()
	}

	// todo: auto merge 选项，检查（上下、左右）相邻的多个格子，如果内容一致则进行合并操作
	//autoMergeExcel(f)

	err = f.SaveAs(fn)
	if err != nil {
		panic(fmt.Errorf("toExcel failed: %w", err))
	}

	AddStaticResource(p, fn)
	return &FuncResult{
		//OutFile: fn,
		Artifacts: []*Artifact{
			{
				FilePath: fn,
				FileType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
			},
		},
	}
}

func formatWriteCell(f *excelize.File, sheetName string, row, cols int, value gjson.Result) (err error) {
	colNo := 'A'

	// 创建空白单元格样式
	style, err := f.NewStyle(&excelize.Style{
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
			WrapText:   true,
		},
	})
	if err != nil {
		fmt.Println(err)
	}

	// 格式化单元格
	err = f.SetColWidth(sheetName, fmt.Sprintf("%c", colNo+int32(cols)),
		fmt.Sprintf("%c", colNo+int32(cols)), 30)
	if err != nil {
		panic(fmt.Errorf("ToExcel SetColWidth failed: %w", err))
	}
	err = f.SetRowHeight(sheetName, row+1, 35)
	if err != nil {
		panic(fmt.Errorf("ToExcel SetRowHeight failed: %w", err))
	}
	err = f.SetCellStyle(sheetName, fmt.Sprintf("%c%d", colNo+int32(cols), row+1),
		fmt.Sprintf("%c%d", colNo+int32(cols), row+1), style)
	if err != nil {
		panic(fmt.Errorf("ToExcel SetCellStyle failed: %w", err))
	}
	// 写入内容
	err = f.SetCellValue(sheetName, fmt.Sprintf("%c%d", colNo+int32(cols), row+1),
		value.String())
	if err != nil {
		return fmt.Errorf("ToExcel SetCellValue failed: %w", err)
	}

	return nil
}

// jsonFormatToExcel 将嵌套json转化为单页的excel格式
func jsonFormatToExcel(f *excelize.File, line string, lineNum int) (err error) {
	currentRow := 0
	v := gjson.ParseBytes([]byte(line))
	rawSheetName := gjson.Get(line, "sheet_name")
	log.Printf("got raw sheet name: %s", rawSheetName.String())
	var sheetName string
	if rawSheetName.Exists() && rawSheetName.String() != "" {
		log.Printf("set sheet name to %s", rawSheetName.String())
		sheetName = rawSheetName.String()
	} else {
		sheetName = fmt.Sprintf("Sheet%d", lineNum)
	}

	log.Printf("start processing sheet name %s", sheetName)

	index := f.NewSheet(sheetName)
	f.SetActiveSheet(index)
	sheetName = f.GetSheetName(index)

	log.Printf("got sheet num %d, with name %s, start processing", index, sheetName)

	// 开始遍历 键值 对应关系
	v.ForEach(func(key, value gjson.Result) bool {
		if key.String() == "sheet_name" {
			return true
		}
		/**
		列表形式：
		 "c_title": [
		        {
		            "count": 27,
		            "name": "DPTECH ONLINE",
		            "source": "fofa.info/stats"
		        },
		        {
		            "count": 12,
		            "name": "HTTP状态 404 - 未找到",
		            "source": "fofa.info/stats"
		        }
		    ]
		|       |count|         name         | source          |
		|c_title| 27  |     DPTECH ONLINE    | fofa.info/stats |
		|		| 12  |HTTP状态 404 - 未找到   | fofa.info/stats |
		*/
		if value.IsArray() {
			cols := 0
			startRow := currentRow
			// 写最左侧的 key, 稍后合并
			err = formatWriteCell(f, sheetName, startRow, cols, key)
			if err != nil {
				log.Printf("write object header cell failed %s", err.Error())
				return false
			}

			// 处理 value with key
			var needMerge bool
			value.ForEach(func(k, v gjson.Result) bool {
				if v.IsObject() {
					needMerge = true
					cols = 0
					// object 按照上述输出
					v.ForEach(func(innerKey, innerValue gjson.Result) bool {
						if currentRow == startRow {
							// 第一行写对应的 key & value
							cols++
							err = formatWriteCell(f, sheetName, currentRow, cols, innerKey)
							if err != nil {
								return false
							}
							err = formatWriteCell(f, sheetName, currentRow+1, cols, innerValue)
							if err != nil {
								return false
							}
							return true
						} else {
							cols++
							// 后面的行只写对应的 value
							err = formatWriteCell(f, sheetName, currentRow, cols, innerValue)
							if err != nil {
								return false
							}
							return true
						}
					})
					if startRow == currentRow {
						currentRow += 2
					} else {
						currentRow += 1
					}
					return true
				} else {
					// 非 object，直接输出
					/**
					列表形式：
					 "c_title": [
					        11,
					        23
					    ]
					|c_title| 11  |    23   |
					*/
					if k.Raw != "" && k.Exists() {
						err = formatWriteCell(f, sheetName, currentRow, cols, k)
						if err != nil {
							return false
						}
						err = formatWriteCell(f, sheetName, currentRow, cols+1, v)
						if err != nil {
							return false
						}
					} else {
						if cols == 0 {
							err = formatWriteCell(f, sheetName, currentRow, cols, key)
							if err != nil {
								return false
							}
							cols++
						}
						err = formatWriteCell(f, sheetName, currentRow, cols, v)
						if err != nil {
							return false
						}
					}
					cols++
					return true
				}
			})

			if needMerge {
				// value 处理完成，合并最左边的标题
				err = f.MergeCell(sheetName, fmt.Sprintf("A%d", startRow+1), fmt.Sprintf("A%d", currentRow))
				if err != nil {
					log.Printf("merge cell failed %s", err.Error())
					return false
				}
			} else {
				currentRow++
			}
		} else if value.IsObject() {
			/**
			object 形式：
			 "location": {
			        "city": "Hangzhou City",
			        "country": "China",
			        ...
			    }
			|        |       city     |    country   |      source     |
			|location| Hangzhou City  |     China    | fofa.info/stats |
			*/
			cols := 0
			// 写最左侧的 key, 并与下一行合并
			err = formatWriteCell(f, sheetName, currentRow, cols, key)
			if err != nil {
				log.Printf("write object header cell failed %s", err.Error())
				return false
			}
			err = f.MergeCell(sheetName, fmt.Sprintf("A%d", currentRow+1), fmt.Sprintf("A%d", currentRow+2))
			if err != nil {
				log.Printf("merge cell failed %s", err.Error())
				return false
			}
			// 写入右边的 object 对应的 key/value
			value.ForEach(func(k, v gjson.Result) bool {
				cols++
				err = formatWriteCell(f, sheetName, currentRow, cols, k)
				if err != nil {
					return false
				}
				err = formatWriteCell(f, sheetName, currentRow+1, cols, v)
				if err != nil {
					return false
				}
				return true
			})
			currentRow += 2
		} else {
			cols := 0
			/**
			flat 正常形式：
			"ip": "122.224.163.198"
			|	ip	 |  122.224.163.198  |
			*/
			// 写最左侧的 key, 并与下一行合并
			err = formatWriteCell(f, sheetName, currentRow, cols, key)
			if err != nil {
				log.Printf("write object header cell failed %s", err.Error())
				return false
			}
			err = formatWriteCell(f, sheetName, currentRow, cols+1, value)
			if err != nil {
				log.Printf("write object value cell failed %s", err.Error())
				return false
			}
			currentRow++
		}

		return true
	})

	return nil
}
