package machinery

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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

func RewritePrintlog(input io.ReadCloser, output io.WriteCloser, catalog *Catalog, list *WTList) {
	defer input.Close()
	defer output.Close()
	scanner := bufio.NewScanner(input)
	scanner.Split(bufio.ScanLines)

	lineNum := 0
	// Track which table a key/value pair are associated with. Customize output for MDB collection and indexes.
	var lastSeenTableName string
	isRowPut := false
	for scanner.Scan() {
		lineNum += 1

		line := scanner.Text()
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
			} else if indexInfo, exists := catalog.FileToIndex[lastSeenTableName]; exists {
				mdbDisplayName = fmt.Sprintf("NS: %s IndexName: %s Spec: %s",
					indexInfo.Owner.Name, indexInfo.Name, indexInfo.Definition)
			} else if IsMdbTable(lastSeenTableName) && lastSeenTableName != "_mdb_catalog" {
				// We could do better here. It's possible the printlog output for the `_mdb_catalog`
				// has an insert for this table name/ident.
				mdbDisplayName = "Unknown (dropped?) table"
			}

			// Reconstitute the ".wt" suffix. I assume it's easier for people to digest that
			// `WiredTiger.wt` is the actual metadata table rather than an ambiguous looking
			// `WiredTiger`.
			if exists {
				output.Write([]byte(fmt.Sprintf("%s.wt %s\n", lastSeenTableName, mdbDisplayName)))
			} else {
				output.Write([]byte("\n"))
			}
		} else if strings.HasPrefix(line, "        \"value-hex\": \"") && IsMdbTable(lastSeenTableName) {
			valueHexStr := line[22 : len(line)-1]
			valueBinary, err := hex.DecodeString(valueHexStr)
			if err != nil {
				panic(err)
			}

			switch {
			case lastSeenTableName == "":
				// Table is unknown because it was no longer in wt list/_mdb_catalog. Try to turn the bytes into bson.
				byt, err := MayMarshal(valueBinary, "        ")
				if err == nil {
					output.Write([]byte("        \"value-bson\": "))
					output.Write(byt)
				} else {
					output.Write([]byte(line))
				}
			case IsCollection(lastSeenTableName) && isRowPut:
				output.Write([]byte("        \"value-bson\": "))
				PPrintExt(output, valueBinary, "        ")
			case IsCollection(lastSeenTableName) && !isRowPut:
				output.Write([]byte(line))
			case IsIndex(lastSeenTableName):
				// ksdecode
				output.Write([]byte(line))
			default:
				panic("Unknown? " + lastSeenTableName)
			}

			output.Write([]byte("\n"))
		} else {
			output.Write([]byte(line))
			output.Write([]byte("\n"))
		}
	}
}
