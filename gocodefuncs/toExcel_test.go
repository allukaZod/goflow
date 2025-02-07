package gocodefuncs

import (
	"bufio"
	"fmt"
	"github.com/LubyRuffy/goflow/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func TestToExcel(t *testing.T) {
	// 读excel-》json
	filename := writeSampleExcelFile()
	fr := ExcelToJson(&testRunner{
		T:        t,
		lastFile: filename,
	}, map[string]interface{}{})

	// json=》写excel
	result := ToExcel(&testRunner{
		T:        t,
		lastFile: fr.OutFile,
	}, map[string]interface{}{
		"rawFormat": true,
	})

	// 再读
	fr = ExcelToJson(&testRunner{
		T:        t,
		lastFile: result.Artifacts[0].FilePath,
	}, map[string]interface{}{})
	f, err := utils.ReadFirstLineOfFile(fr.OutFile)
	assert.Nil(t, err)
	assert.Equal(t, string(f), `{"Sheet1":[["IP","域名"],["1.1.1.1","a.com"]],"Sheet2":[null,["Hello world."]]}`)

	// json=》写excel
	json := `{"Sheet1":[["IP"],["1.1.1.1"],["2.2.2.2"]]}`
	fn, err := utils.WriteTempFile(".json", func(f *os.File) error {
		_, err := f.WriteString(json)
		return err
	})
	result = ToExcel(&testRunner{
		T:        t,
		lastFile: fn,
	}, map[string]interface{}{
		"rawFormat": true,
	})
	// 再读
	fr = ExcelToJson(&testRunner{
		T:        t,
		lastFile: result.Artifacts[0].FilePath,
	}, map[string]interface{}{})
	f, err = utils.ReadFirstLineOfFile(fr.OutFile)
	assert.Nil(t, err)
	assert.Equal(t, string(f), `{"Sheet1":[["IP"],["1.1.1.1"],["2.2.2.2"]]}`)

	// 写合并的表格
	json = `{"Sheet1":[["t1","t2","t3"],["dct11","dct12","dct13"],["dct11","dct22","dct13"]],"Sheet2":[["t1","t2","t3"],["dct11","dct11","dct12"],["dct11","dct11","dct22"]],"_merged_Sheet1":[["A2:A3","dct11"],["C2:C3","dct13"]],"_merged_Sheet2":[["A2:B3","dct11"]]}`
	fn, err = utils.WriteTempFile(".json", func(f *os.File) error {
		_, err := f.WriteString(json)
		return err
	})
	result = ToExcel(&testRunner{
		T:        t,
		lastFile: fn,
	}, map[string]interface{}{
		"rawFormat": true,
	})
	// 再读
	fr = ExcelToJson(&testRunner{
		T:        t,
		lastFile: result.Artifacts[0].FilePath,
	}, map[string]interface{}{})
	f, err = utils.ReadFirstLineOfFile(fr.OutFile)
	assert.Nil(t, err)
	assert.Equal(t, string(f), `{"Sheet1":[["t1","t2","t3"],["dct11","dct12","dct13"],["dct11","dct22","dct13"]],"Sheet2":[["t1","t2","t3"],["dct11","dct11","dct12"],["dct11","dct11","dct22"]],"_merged_Sheet1":[["A2:A3","dct11"],["C2:C3","dct13"]],"_merged_Sheet2":[["A2:B3","dct11"]]}`)
}

func TestToExcel1(t *testing.T) {
	type args struct {
		params    map[string]interface{}
		inputJson string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "测试raw格式输入",
			args: args{
				params: map[string]interface{}{
					"rawFormat": true,
					"insertPic": false,
				},
				inputJson: "{\"Sheet1\":[[\"IP\",\"47.106.125.91\",\"47.106.125.91\",\"47.106.125.91\",\"47.106.125.91\"],[\"地理位置\",\"广东省广州市\",\"注册机构\",\"阿里云\",\"阿里云\"],[\"网络类型\",\"数据中心\",\"经纬度\",\"113.307650,23.120049\",\"113.307650,23.120049\"],[\"ASN\",\"AS37963CNNIC-ALIBABA-CN-NET-AP\",\"IDC服务器\",\"是\",\"是\"],[\"网络资产\",\"22\",\"TCP/SSH\",\"OpenSSH\",\"SSH-2.0-OpenSSH_7.4\"],[\"网络资产\",\"443\",\"TCP/HTTP\",\"NGINX,digicert-Cert\",\"HTTP/1.1502BadGateway\\nConnection:close\"],[\"网络资产\",\"80\",\"TCP/HTTP\",\"NGINX\",\"HTTP/1.1200OK\\nConnection:close\"],[\"网络资产\",\"9200\",\"TCP/ELASTIC\",\"Log4j2,Elasticsearch\",\"HTTP/1.0200OK\\nX-elastic-product:Elasticsearch\"],[\"网络资产\",\"11001\",\"TCP/HTTP\",\"\",\"HTTP/1.1400BadRequest\\nContent-Type:text/html;charset=us-ascii\"],[\"网络资产\",\"3000\",\"TCP/HTTP\",\"\",\"HTTP/1.1200OK\\nAccess-Control-Allow-Origin:*\"],[\"网络资产\",\"6001\",\"TCP/UNKNOW\",\"\",\"\\\\x00\\\\x00\\\\x00\\\\x01\\\\x00\"],[\"网络资产\",\"9003\",\"TCP/HTTP\",\"\",\"HTTP/1.1200OK\\nConnection:close\"],[\"根域资产（半年内）\",\"enyamusic.cn\",\"\",\"postmaster@enyamusic.pro\",\"广州恩雅创新科技有限公司\"],[\"根域资产（半年内）\",\"makedingge.com\",\"+86.95187\",\"DomainAbuse@service.aliyun.com\"],[\"IP情报\",\"02-17-17\",\"06-01-18\",\"垃圾邮件\",\"过期\"],[\"Web页面\",\"443\",\"资产测绘标题（502BadGateway）\",\"目前标题（502BadGateway）\"],[\"Web页面\",\"80\",\"资产测绘标题（Welcometonginx!）\",\"目前标题（Welcometonginx!）\"],[\"Web页面\",\"11001\",\"资产测绘标题（无）\",\"目前标题（无）\",\"无法截图\"],[\"Web页面\",\"3000\",\"资产测绘标题（无）\",\"目前标题（无）\",\"无法截图\"],[\"Web页面\",\"9003\",\"资产测绘标题（无）\",\"目前标题（无）\",\"无法截图\"],[\"证书\",\"443\",\"\",\"*.enyamusic.cn\",\"*.enyamusic.cn\"],[\"C段Web资产标题排序\",\"口袋客app（771）\",\"口袋客app（771）\",\"口袋客app（771）\",\"口袋客app（771）\"],[\"C段Web资产标题排序\",\"301MovedPermanently（50）\",\"301MovedPermanently（50）\",\"301MovedPermanently（50）\",\"301MovedPermanently（50）\"],[\"C段Web资产标题排序\",\"403Forbidden（49）\",\"403Forbidden（49）\",\"403Forbidden（49）\",\"403Forbidden（49）\"],[\"C段Web资产标题排序\",\"安全入口校验失败（26）\",\"安全入口校验失败（26）\",\"安全入口校验失败（26）\",\"安全入口校验失败（26）\"],[\"C段Web资产标题排序\",\"NotFound（21）\",\"NotFound（21）\",\"NotFound（21）\",\"NotFound（21）\"],[\"IP和域名网络情报\",\"enyamusic.cn\",\"sadf\",\"sadf\",\"sadf\"],[\"IP和域名网络情报\",\"makedingge.com\",\"asadf\",\"asadf\",\"asadf\"],[\"IP和域名网络情报\",\"103.117.102.89\",\"fdfasf\",\"fdfasf\",\"fdfasf\"]],\"_merged_Sheet1\":[[\"A27:A29\",\"IP和域名网络情报\"],[\"C27:E27\",\"sadf\"],[\"C28:E28\",\"asadf\"],[\"C29:E29\",\"fdfasf\"],[\"A22:A26\",\"C段Web资产标题排序\"],[\"A13:A14\",\"根域资产（半年内）\"],[\"D21:E21\",\"*.enyamusic.cn\"],[\"B26:E26\",\"NotFound（21）\"],[\"A16:A20\",\"Web页面\"],[\"B25:E25\",\"安全入口校验失败（26）\"],[\"B24:E24\",\"403Forbidden（49）\"],[\"B23:E23\",\"301MovedPermanently（50）\"],[\"B22:E22\",\"口袋客app（771）\"],[\"D3:E3\",\"113.307650,23.120049\"],[\"B1:E1\",\"47.106.125.91\"],[\"D4:E4\",\"是\"],[\"D2:E2\",\"阿里云\"],[\"A5:A12\",\"网络资产\"]]}",
			},
		},
		{
			name: "测试json自动格式化",
			args: args{
				params: map[string]interface{}{
					"rawFormat":  false,
					"insertPic":  false,
					"jsonFormat": true,
				},
				inputJson: "{\"ip\":\"122.224.163.198\",\"location\":{\"city\":\"Hangzhou City\",\"country\":\"China\",\"country_code\":\"CN\",\"lat\":\"30.384272\",\"lng\":\"119.987002\",\"province\":\"Zhejiang\",\"source\":\"threatbook.cn/ip\"},\"asn\":{\"source\":\"fofa.info/host\",\"value\":4134},\"org\":{\"source\":\"fofa.info/host\",\"value\":\"Chinanet\"},\"judgements\":{\"value\":[\"Gateway\",\"Exploit\"],\"source\":\"threatbook.cn/ip\"},\"c_title\":[{\"count\":27,\"name\":\"DPTECH ONLINE\",\"source\":\"fofa.info/stats\"},{\"count\":21,\"name\":\"Welcome to tengine!\",\"source\":\"fofa.info/stats\"},{\"count\":20,\"name\":\"域名暂未生效\",\"source\":\"fofa.info/stats\"},{\"count\":18,\"name\":\"建设中\",\"source\":\"fofa.info/stats\"},{\"count\":12,\"name\":\"HTTP状态 404 - 未找到\",\"source\":\"fofa.info/stats\"}],\"ports\":[{\"port\":8999,\"products\":[\"Oracle-JSP\",\"泛微-EMobile\",\"Log4j2\"],\"protocol\":\"http\",\"source\":\"fofa.info/v1/host\",\"update_time\":\"2022-06-11 11:00:00\"},{\"port\":9000,\"products\":[\"泛微-EMobile\",\"Log4j2\",\"NGINX\",\"Oracle-JSP\"],\"protocol\":\"http\",\"source\":\"fofa.info/v1/host\",\"update_time\":\"2022-06-03 21:00:00\"},{\"port\":2223,\"products\":[\"ubuntu-系统\",\"OpenSSH\"],\"protocol\":\"ssh\",\"source\":\"fofa.info/v1/host\",\"update_time\":\"2022-06-18 07:00:00\"},{\"port\":9090,\"products\":[\"Oracle-JSP\",\"jQuery\",\"泛微-E-Weaver\",\"Log4j2\"],\"protocol\":\"http\",\"source\":\"fofa.info/v1/host\",\"update_time\":\"2022-06-01 06:00:00\"}],\"threatbook_lab\":[{\"confidence\":75,\"expired\":false,\"find_time\":\"2021-10-25 21:30:23\",\"intel_tags\":[],\"intel_types\":[\"Exploit\"],\"source\":\"threatbook.cn/ip\",\"update_time\":\"2023-05-31 01:12:26\"},{\"confidence\":80,\"expired\":false,\"find_time\":\"2021-07-26 05:38:17\",\"intel_tags\":[],\"intel_types\":[\"Dynamic IP\"],\"source\":\"threatbook.cn/ip\",\"update_time\":\"2023-02-17 07:33:12\"},{\"confidence\":85,\"expired\":false,\"find_time\":\"2020-04-24 02:24:01\",\"intel_tags\":[],\"intel_types\":[\"Gateway\"],\"source\":\"threatbook.cn/ip\",\"update_time\":\"2023-05-29 19:19:42\"},{\"confidence\":85,\"expired\":false,\"find_time\":\"2023-04-21 01:15:58\",\"intel_tags\":[[\"CVE-2022-22965\",\"Spring4Exp\"]],\"intel_types\":[\"Exploit\"],\"source\":\"threatbook.cn/ip\",\"update_time\":\"2023-04-21 01:15:57\"}]}",
			},
		},
		{
			name: "测试json自动格式化， 列表",
			args: args{
				params: map[string]interface{}{
					"rawFormat":  true,
					"insertPic":  false,
					"jsonFormat": false,
				},
				inputJson: "{\"ips\": [\"123\", \"12345\",\"666\"]}",
			},
		},
		{
			name: "测试自动化生成 样本信息",
			args: args{
				params: map[string]interface{}{
					"rawFormat":  true,
					"insertPic":  false,
					"jsonFormat": false,
				},
				inputJson: "{\"文件信息\":{\"文件名\":\"/tmp/eml_attach_for_scan/865da724f20741496cfa9c9e08a83358.file\",\"文件格式\":\"DOCX\",\"文件大小\":\"90906字节\"},\"样本哈希\":{\"MD5\":\"865da724f20741496cfa9c9e08a83358\",\"SHA-1\":\"cf0ce70640390c36d78b6791f7cba85b2fc55515\",\"SHA-256\":\"af988d92c694d2fdc113154b79f4fbbf8e5e78d0ec026bffedb264522001fba2\"},\"研判标签\":[{\"标签\":\"\",\"来源\":\"ti.360.net/hash_info\"},{\"标签\":\"trojan.\",\"来源\":\"VirusTotal\"}],\"360研判情报\":{\"所属家族\":[],\"恶意软件类型\":[\"Trojan\",\"木马\"],\"其他标签\":[\"Office/Trojan.Generic.GjcATDoA\"],\"威胁类型\":\"Trojan\",\"来源\":\"ti.360.net/hash_info\"},\"微步研判情报\":{\"所属家族\":\"\",\"恶意软件类型\":\"\",\"是否为恶意软件\":\"clean\",\"更新时间\":\"2023-05-18 04:51:06\",\"来源\":\"threatbook.cn/file\"},\"奇安信研判情报\":{\"样本家族\":\"\",\"恶意软件类型\":\"trojan\",\"所属组织\":\"\",\"发现时间\":\"2023-05-17 18:54:48\",\"最后分析时间\":\"2023-06-06 11:48:36\"},\"引擎检测率\":{\"VT\":\"11/63\",\"微步\":\"2/24\"},\"Yara 匹配规则\":[{\"作者\":\"InQuest Labs\",\"描述\":\"This signature identifies Adobe Extensible Metadata Platform (XMP) identifiers embedded within files. Defined as a standard for mapping graphical asset relationships, XMP allows for tracking of both parent-child relationships and individual revisions. There are three categories of identifiers: original document, document, and instance. Generally, XMP data is stored in XML format, updated on save/copy, and embedded within the graphical asset. These identifiers can be used to track both malicious and benign graphics within common Microsoft and Adobe document lures.\",\"来源\":\"https://github.com/InQuest/yara-rules-vt\",\"规则名\":\"Adobe_XMP_Identifier\",\"规则集\":\"Adobe_XMP_Identifier\"}],\"sheet_name\":\"865da724f20741496cfa9c9e08a83358\"}\n{\"文件信息\":{\"文件名\":\"申请时间及申请入口.docx\",\"文件格式\":\"DOCX\",\"文件大小\":\"57906字节\"},\"样本哈希\":{\"MD5\":\"be11f1e5eab0c3904a7ab8986e1d200f\",\"SHA-1\":\"cb3d9264db159c484afb2eda7df4ec5860e4e6ff\",\"SHA-256\":\"9617e69049902d1931a8355fe5c549cc87dc9cece344f93bed41cae388b8f4f5\"},\"研判标签\":[{\"标签\":\"\",\"来源\":\"ti.360.net/hash_info\"},{\"标签\":{\"error\":\"missing\"},\"来源\":\"VirusTotal\"}],\"360研判情报\":{\"所属家族\":[],\"恶意软件类型\":[],\"其他标签\":[],\"威胁类型\":\"\",\"来源\":\"ti.360.net/hash_info\"},\"微步研判情报\":{\"所属家族\":\"\",\"恶意软件类型\":\"\",\"是否为恶意软件\":\"clean\",\"更新时间\":\"2023-05-14 23:50:46\",\"来源\":\"threatbook.cn/file\"},\"奇安信研判情报\":{\"样本家族\":\"\",\"恶意软件类型\":\"\",\"所属组织\":\"\",\"发现时间\":\"2023-05-14 16:48:15\",\"最后分析时间\":\"2023-06-17 01:27:18\"},\"引擎检测率\":{\"VT\":\"2/63\",\"微步\":\"1/24\"},\"Yara 匹配规则\":[{\"作者\":\"InQuest Labs\",\"描述\":\"This signature identifies Adobe Extensible Metadata Platform (XMP) identifiers embedded within files. Defined as a standard for mapping graphical asset relationships, XMP allows for tracking of both parent-child relationships and individual revisions. There are three categories of identifiers: original document, document, and instance. Generally, XMP data is stored in XML format, updated on save/copy, and embedded within the graphical asset. These identifiers can be used to track both malicious and benign graphics within common Microsoft and Adobe document lures.\",\"来源\":\"https://github.com/InQuest/yara-rules-vt\",\"规则名\":\"Adobe_XMP_Identifier\",\"规则集\":\"Adobe_XMP_Identifier\"}],\"sheet_name\":\"be11f1e5eab0c3904a7ab8986e1d200f\"}\n{\"文件信息\":{\"文件名\":\"2023二季度工资补贴申领.docx\",\"文件格式\":\"DOCX\",\"文件大小\":\"106856字节\"},\"样本哈希\":{\"MD5\":\"944bc7f8839a092f51857024745c0bd6\",\"SHA-1\":\"8762bab83fbba1af736e3a1b3a685cd65fcd238a\",\"SHA-256\":\"a072088bda32586713b4195116387f4dab665add42fe743308078dbd4182394b\"},\"研判标签\":[{\"标签\":\"\",\"来源\":\"ti.360.net/hash_info\"},{\"标签\":{\"error\":\"missing\"},\"来源\":\"VirusTotal\"}],\"360研判情报\":{\"所属家族\":[],\"恶意软件类型\":[],\"其他标签\":[],\"威胁类型\":\"\",\"来源\":\"ti.360.net/hash_info\"},\"微步研判情报\":{\"所属家族\":\"\",\"恶意软件类型\":\"\",\"是否为恶意软件\":\"clean\",\"更新时间\":\"2023-05-15 02:24:10\",\"来源\":\"threatbook.cn/file\"},\"奇安信研判情报\":{\"样本家族\":\"\",\"恶意软件类型\":\"\",\"所属组织\":\"\",\"发现时间\":\"2023-05-14 18:14:49\",\"最后分析时间\":\"2023-06-05 10:59:43\"},\"引擎检测率\":{\"VT\":\"2/62\",\"微步\":\"1/24\"},\"Yara 匹配规则\":[{\"作者\":\"InQuest Labs\",\"描述\":\"This signature identifies Adobe Extensible Metadata Platform (XMP) identifiers embedded within files. Defined as a standard for mapping graphical asset relationships, XMP allows for tracking of both parent-child relationships and individual revisions. There are three categories of identifiers: original document, document, and instance. Generally, XMP data is stored in XML format, updated on save/copy, and embedded within the graphical asset. These identifiers can be used to track both malicious and benign graphics within common Microsoft and Adobe document lures.\",\"来源\":\"https://github.com/InQuest/yara-rules-vt\",\"规则名\":\"Adobe_XMP_Identifier\",\"规则集\":\"Adobe_XMP_Identifier\"}],\"sheet_name\":\"944bc7f8839a092f51857024745c0bd6\"}\n{\"文件信息\":{\"文件名\":\"PURCHASE ORDERBusiness Doc.htm\",\"文件格式\":\"HTML\",\"文件大小\":\"1815字节\"},\"样本哈希\":{\"MD5\":\"98bb5bc4b71b3dcfad903338994f714a\",\"SHA-1\":\"dd8f9e208b4d7b066345f07962a61f770f77151c\",\"SHA-256\":\"a5fa923d109565322005d39814ec433e5b9f14d690fb8db2653cc85aebd40618\"},\"研判标签\":[{\"标签\":\"\",\"来源\":\"ti.360.net/hash_info\"},{\"标签\":\"phishing.phishingdpx\",\"来源\":\"VirusTotal\"}],\"360研判情报\":{\"所属家族\":[],\"恶意软件类型\":[\"木马\",\"Trojan\"],\"其他标签\":[\"Generic/Trojan.Generic.HkcATCcA\"],\"威胁类型\":\"Trojan\",\"来源\":\"ti.360.net/hash_info\"},\"微步研判情报\":{\"所属家族\":\"Phishing\",\"恶意软件类型\":\"Trojan\",\"是否为恶意软件\":\"malicious\",\"更新时间\":\"2023-05-18 18:44:09\",\"来源\":\"threatbook.cn/file\"},\"奇安信研判情报\":{\"样本家族\":\"\",\"恶意软件类型\":\"trojan\",\"所属组织\":\"\",\"发现时间\":\"2023-05-18 10:15:17\",\"最后分析时间\":\"2023-05-19 06:25:28\"},\"引擎检测率\":{\"VT\":\"11/60\",\"微步\":\"6/24\"},\"Yara 匹配规则\":[],\"sheet_name\":\"98bb5bc4b71b3dcfad903338994f714a\"}\n{\"文件信息\":{\"文件名\":\"Autovermietung.exe\",\"文件格式\":\"Win64 EXE\",\"文件大小\":\"379904字节\"},\"样本哈希\":{\"MD5\":\"143745981a97902ad93e21b460e16536\",\"SHA-1\":\"71d500a62fc19b0e6975d85dddd02c533edea790\",\"SHA-256\":\"574d698e2096e5db6720f04900b9a8639eee9b0e2bbf52c9b5ab958a75ec0809\"},\"研判标签\":[{\"标签\":\"\",\"来源\":\"ti.360.net/hash_info\"},{\"标签\":\"trojan.msil/snakelogger\",\"来源\":\"VirusTotal\"}],\"360研判情报\":{\"所属家族\":[],\"恶意软件类型\":[\"TrojanSpy\",\"木马\"],\"其他标签\":[\"Win64/TrojanSpy.Generic.HgEATCYA\"],\"威胁类型\":\"TrojanSpy\",\"来源\":\"ti.360.net/hash_info\"},\"微步研判情报\":{\"所属家族\":\"Kryptik\",\"恶意软件类型\":\"Trojan\",\"是否为恶意软件\":\"malicious\",\"更新时间\":\"2023-05-28 15:30:00\",\"来源\":\"threatbook.cn/file\"},\"奇安信研判情报\":{\"样本家族\":\"Zbot\",\"恶意软件类型\":\"trojan\",\"所属组织\":\"\",\"发现时间\":\"2023-05-17 08:42:39\",\"最后分析时间\":\"2023-05-25 05:36:09\"},\"引擎检测率\":{\"VT\":\"39/63\",\"微步\":\"8/24\"},\"Yara 匹配规则\":[],\"sheet_name\":\"143745981a97902ad93e21b460e16536\"}",
			},
		},
		{
			name: "测试图片插入",
			args: args{params: map[string]interface{}{
				"rawFormat":  false,
				"insertPic":  true,
				"jsonFormat": false,
			},
				inputJson: "{\"url\":\"http://www.123.com\",\"1_filepath\":\"1.png\",\"2_filepath\":\"2.png\"}",
			},
		},
		{
			name: "测试json自动格式化，列表格式化",
			args: args{params: map[string]interface{}{
				"jsonFormat": true,
			},
				inputJson: "{\"月更新总数\": {\"fofa\": 943,\"zoomeye\": 335,\"hunter\": 7500,\"quake\": 2217},\"年更新总数\": [4830, 2530, 14225, 7509]}",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestRunner(t, tt.args.inputJson)
			p.SetObject(utils.ResourceFieldsObjectName, []string{"1_filepath", "2_filepath"})
			p.SetObject(utils.ResourcesObjectName, []string{"1.png", "2.png"})
			got := ToExcel(p, tt.args.params)
			assert.NotNil(t, got.Artifacts)
			t.Logf("output file: %s", got.Artifacts[0].FilePath)
		})
	}
}

func TestToExcelBenchmark(t *testing.T) {
	benchmark := testing.Benchmark(func(b *testing.B) {
		// 读取文件 toExcel_input.json 逐行读取json数据，写到 inputJson 中
		jsonLines, err := readJSONLines("toExcel_input.json")
		if err != nil {
			t.Fatalf("无法读取 JSON 文件: %v", err)
		}

		inputJson := strings.Join(jsonLines, "\n")
		p := newTestRunner(t, inputJson)
		got := ToExcel(p, map[string]interface{}{
			"rawFormat":  false,
			"insertPic":  false,
			"jsonFormat": false,
		})
		assert.NotNil(t, got.Artifacts)
		t.Logf("output file: %s", got.Artifacts[0].FilePath)
	})
	fmt.Println(benchmark)
}

func readJSONLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取文件时发生错误: %v", err)
	}

	return lines, nil
}
