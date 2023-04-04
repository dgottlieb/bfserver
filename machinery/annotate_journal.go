package machinery

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

func PPrint(thing interface{}) string {
	_ = fmt.Println
	jsonStr, err := json.MarshalIndent(thing, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(jsonStr)
}

func PPrintExt(writer io.Writer, buf []byte, prefix string) {
	var valBson bson.D
	bson.Unmarshal(buf, &valBson)
	str, err := bson.MarshalExtJSONIndent(valBson, false, false, prefix, "  ")
	if err != nil {
		panic(err)
	}
	writer.Write(str)
}

func MayMarshal(buf []byte, prefix string) ([]byte, error) {
	var valBson bson.D
	bson.Unmarshal(buf, &valBson)
	byt, err := bson.MarshalExtJSONIndent(valBson, false, false, prefix, "  ")
	if err != nil {
		return []byte{}, err
	}
	return byt, nil
}

type IndexInfo struct {
	Name       string
	Ident      string
	Definition string

	Owner *CollectionInfo `json:"-"`
}

type CollectionInfo struct {
	Name            string
	Ident           string
	IndexNameToInfo map[string]*IndexInfo
}

type Catalog struct {
	FileToCollection map[string]*CollectionInfo
	FileToIndex      map[string]*IndexInfo
	Collections      []*CollectionInfo
	Indexes          []*IndexInfo
}

type MdbCatalogFormat struct {
	Ns       string
	Ident    string
	IdxIdent map[string]string
	Metadata struct {
		Indexes []struct {
			Spec struct {
				Key  bson.D
				Name string
			}
		}
	} `bson:"md"`
}

func (catalog *Catalog) AddRow(inp *MdbCatalogFormat) {
	cinfo := &CollectionInfo{
		Name:            inp.Ns,
		Ident:           inp.Ident,
		IndexNameToInfo: make(map[string]*IndexInfo),
	}

	for idxName, idxIdent := range inp.IdxIdent {
		iinfo := &IndexInfo{
			Name:  idxName,
			Ident: idxIdent,
			Owner: cinfo,
		}
		cinfo.IndexNameToInfo[idxName] = iinfo
		catalog.Indexes = append(catalog.Indexes, iinfo)
		catalog.FileToIndex[idxIdent] = iinfo
	}

	for _, index := range inp.Metadata.Indexes {
		specStr, err := bson.MarshalExtJSON(index.Spec.Key, false, false)
		if err != nil {
			panic(err)
		}
		cinfo.IndexNameToInfo[index.Spec.Name].Definition = string(specStr)
	}

	catalog.Collections = append(catalog.Collections, cinfo)
	catalog.FileToCollection[inp.Ident] = cinfo
}

func LoadCatalog(catalogFile io.ReadCloser, annotateWriter io.WriteCloser) *Catalog {
	scanner := bufio.NewScanner(catalogFile)
	scanner.Split(bufio.ScanLines)
	defer catalogFile.Close()
	defer annotateWriter.Close()

	for scanner.Scan() {
		annotateWriter.Write([]byte(scanner.Text()))
		annotateWriter.Write([]byte("\n"))
		if scanner.Text() == "Data" {
			break
		}
	}

	ret := &Catalog{
		FileToCollection: make(map[string]*CollectionInfo),
		FileToIndex:      make(map[string]*IndexInfo),
		Collections:      make([]*CollectionInfo, 0),
		Indexes:          make([]*IndexInfo, 0),
	}
	for {
		more := scanner.Scan()
		if !more {
			break
		}
		annotateWriter.Write([]byte(scanner.Text()))
		annotateWriter.Write([]byte("\n"))

		scanner.Scan()
		value := scanner.Text()
		valBytes, err := hex.DecodeString(value)
		if err != nil {
			panic(err)
		}

		PPrintExt(annotateWriter, valBytes, "")
		annotateWriter.Write([]byte("\n"))

		var parsedFormat MdbCatalogFormat
		if err := bson.Unmarshal(valBytes, &parsedFormat); err != nil {
			panic(err)
		}

		ret.AddRow(&parsedFormat)
	}

	return ret
}

type WTList struct {
	TableToFileId map[string]int64
	FileIdToTable map[int64]string
}

var fileIdRe *regexp.Regexp = regexp.MustCompile(",id=(\\d+),")

func LoadWTList(listFile io.ReadCloser) *WTList {
	scanner := bufio.NewScanner(listFile)
	scanner.Split(bufio.ScanLines)
	defer listFile.Close()

	ret := &WTList{
		TableToFileId: make(map[string]int64),
		FileIdToTable: make(map[int64]string),
	}
	ret.TableToFileId["WiredTiger"] = 0
	ret.FileIdToTable[0] = "WiredTiger"
	for {
		more := scanner.Scan()
		if !more {
			break
		}
		key := scanner.Text()
		if !strings.HasPrefix(key, "file:") {
			continue
		}
		// Trim off the `file:` prefix and the `.wt` suffix.
		tableName := key[5 : len(key)-3]

		scanner.Scan()
		value := scanner.Text()
		fileIdStr := fileIdRe.FindStringSubmatch(value)[1]
		fileIdInt, err := strconv.Atoi(fileIdStr)
		if err != nil {
			panic(err)
		}

		ret.TableToFileId[tableName] = int64(fileIdInt)
		ret.FileIdToTable[int64(fileIdInt)] = tableName
	}

	return ret
}

func IsCollection(tableName string) bool {
	return strings.HasPrefix(tableName, "collection-") || tableName == "_mdb_catalog"
}

func IsIndex(tableName string) bool {
	return strings.HasPrefix(tableName, "index-")
}

func IsMdbTable(tableName string) bool {
	return IsCollection(tableName) || IsIndex(tableName) || tableName == ""
}

func Feed(stdin io.Writer, stdout io.Reader, keystring string) string {
	stdin.Write([]byte(keystring + "\n"))

	result, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		panic(err)
	}

	return result
}

type History struct {
	Lines []string
	ptr   int
}

func NewHistory(cap int) *History {
	return &History{
		Lines: make([]string, cap, cap),
		ptr:   0,
	}
}

func (history *History) Add(lineNum int, line string) {
	history.Lines[history.ptr] = fmt.Sprintf("%6d: %s", lineNum, line)
	history.ptr++

	if history.ptr == len(history.Lines) {
		history.ptr = 0
	}
}

func (history *History) Dump() {
	fmt.Println("History:")
	for num := 0; num < cap(history.Lines); num++ {
		idx := (num + history.ptr) % cap(history.Lines)
		fmt.Println("\t" + history.Lines[idx])
	}
}

func RewritePrintlog(input io.ReadCloser, output io.WriteCloser, catalog *Catalog, list *WTList) {
	defer input.Close()
	defer output.Close()

	ksdecodeCmd := exec.Command("/home/dgottlieb/xgen/mongo/bin/ksdecode", "-o", "bson", "-a")
	ksdecodeStdin, err := ksdecodeCmd.StdinPipe()
	if err != nil {
		panic(err)
	}
	ksdecodeStdout, err := ksdecodeCmd.StdoutPipe()
	if err != nil {
		panic(err)
	}

	if err := ksdecodeCmd.Start(); err != nil {
		panic(err)
	}
	defer func() {
		ksdecodeStdin.Close()
		ksdecodeStdout.Close()
		ksdecodeCmd.Wait()
	}()

	scanner := bufio.NewScanner(input)
	scanner.Split(bufio.ScanLines)

	history := NewHistory(20)
	lineNum := 0
	// Track which table a key/value pair are associated with. Customize output for MDB collection and indexes.
	var lastSeenTableName string
	isRowPut := false
	var lastSeenIndexInfo *IndexInfo
	for scanner.Scan() {
		lineNum += 1

		line := scanner.Text()
		history.Add(lineNum, line)

		switch {
		case strings.HasPrefix(line, "      { \"optype\": \"row_put\""):
			isRowPut = true
		case strings.HasPrefix(line, "      { \"optype\": \"row_modify\""):
			isRowPut = false
		case strings.HasPrefix(line, "        \"key\": "):
			continue
		case strings.HasPrefix(line, "        \"value\": "):
			continue
		}

		// `"fileid" :` (note the space before the colon) is part of a sync record.
		// `"fileid":` (note the lack of space) is part of a txn's row_put/row_remove record.
		if strings.HasPrefix(line, "        \"fileid\":") {
			idHexRe := regexp.MustCompile(" 0x([a-f0-9]+),")
			// fmt.Printf("FileidLine: `%v`\n", line)
			fileHex := idHexRe.FindStringSubmatch(line)[1]

			// WT's "debug" log records will write no-op entries for tables that are not logged for recovery. These no-ops are significant by setting a leading bit (e.g: the `8 in hex).
			// Searching for `8`, strictly speaking, is incorrect. A `9` should* be replaced by a `1`. A problem for a later person
			if fileHex[0] == '8' {
				// e.g: `0x(80000003)`. Note this is 8 hex characters.
				fileHex = strings.Replace(fileHex, "8", "0", 1)
			} else if len(fileHex) == 8 && (fileHex[0] == '9' || (fileHex[0] >= 'a' && fileHex[0] <= 'f')) {
				panic("Smarter stripping of the leading bit is not implemented.")
			}
			// fmt.Printf("0 == 8? %v\n", fileHex[0] == '8')

			fileInt, err := strconv.ParseInt(fileHex, 16, 64)
			if err != nil {
				panic(err)
			}

			output.Write([]byte(line))

			var exists bool
			lastSeenTableName, exists = list.FileIdToTable[fileInt]

			var mdbDisplayName string
			if collInfo, exists := catalog.FileToCollection[lastSeenTableName]; exists {
				mdbDisplayName = collInfo.Name
				lastSeenIndexInfo = nil
			} else if indexInfo, exists := catalog.FileToIndex[lastSeenTableName]; exists {
				mdbDisplayName = fmt.Sprintf("NS: %s IndexName: %s Spec: %s",
					indexInfo.Owner.Name, indexInfo.Name, indexInfo.Definition)
				lastSeenIndexInfo = indexInfo
			} else if IsMdbTable(lastSeenTableName) && lastSeenTableName != "_mdb_catalog" {
				// We could do better here. It's possible the printlog output for the `_mdb_catalog`
				// has an insert for this table name/ident.
				mdbDisplayName = "Unknown (dropped?) table"
				lastSeenIndexInfo = nil
			}

			// Reconstitute the ".wt" suffix. I assume it's easier for people to digest that
			// `WiredTiger.wt` is the actual metadata table rather than an ambiguous looking
			// `WiredTiger`.
			if exists {
				output.Write([]byte(fmt.Sprintf(" %s.wt %s\n", lastSeenTableName, mdbDisplayName)))
			} else {
				output.Write([]byte("\n"))
			}
		} else if strings.HasPrefix(line, "        \"value-hex\": \"") && IsMdbTable(lastSeenTableName) {
			valueHexStr := line[22 : len(line)-1]

			switch {
			case lastSeenTableName == "" && isRowPut:
				valueBinary, err := hex.DecodeString(valueHexStr)
				if err != nil {
					panic(err)
				}

				// Table is unknown because it was no longer in wt list/_mdb_catalog. Try to turn the bytes into bson.
				byt, err := MayMarshal(valueBinary, "        ")
				if err == nil {
					output.Write([]byte("        \"value-bson\": "))
					output.Write(byt)
				} else {
					output.Write([]byte(line))
				}
			case IsCollection(lastSeenTableName) && isRowPut:
				valueBinary, err := hex.DecodeString(valueHexStr)
				if err != nil {
					panic(err)
				}

				output.Write([]byte("        \"value-bson\": "))
				PPrintExt(output, valueBinary, "        ")
			case IsIndex(lastSeenTableName):
				output.Write([]byte(line))
			default:
				history.Dump()
				panic("Unknown? " + lastSeenTableName)
			}

			output.Write([]byte("\n"))
		} else if strings.HasPrefix(line, "        \"key-hex\": \"") && IsIndex(lastSeenTableName) {
			if lastSeenIndexInfo == nil || lastSeenIndexInfo.Name != "_id_" {
				output.Write([]byte(line + "\n"))
				continue
			}

			var keyHexStr string
			if strings.HasSuffix(line, "\",") {
				keyHexStr = line[20 : len(line)-2]
			} else {
				// `row_remove` operations do not have a value. The `key-hex` field is the last
				// element in the item.
				keyHexStr = line[20 : len(line)-1]
			}

			// fmt.Println("KeyHex:", keyHexStr)
			keystring := Feed(ksdecodeStdin, ksdecodeStdout, keyHexStr)
			// fmt.Println("KS:", keystring)
			output.Write([]byte(line + "\n"))
			// Note that the keystring output comes with a tailing newline.
			output.Write([]byte(fmt.Sprintf("        \"Keystring\": %s", FormatKS(keystring))))
		} else {
			output.Write([]byte(line))
			output.Write([]byte("\n"))
		}
	}
}

func FormatKS(keystring string) string {
	// Inp: 5a1004c72cd25367034a5c98067698e8d95e0f04 { : UUID("c72cd253-6703-4a5c-9806-7698e8d95e0f") }
	// Out: { : UUID("c72cd253-6703-4a5c-9806-7698e8d95e0f") }
	_, formatted, _ := strings.Cut(keystring, " ")
	return formatted
}
