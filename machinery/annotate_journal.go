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

func PPrintExt(writer io.Writer, buf []byte) {
	var valBson bson.D
	bson.Unmarshal(buf, &valBson)
	str, err := bson.MarshalExtJSONIndent(valBson, false, false, "", "  ")
	if err != nil {
		panic(err)
	}
	writer.Write(str)
	writer.Write([]byte("\n"))
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
	Ns string
	Ident string
	IdxIdent map[string]string
	Metadata struct {
		Indexes []struct {
			Spec struct {
				Key bson.D
				Name string
			}
		}
	} `bson:"md"`
}

func (catalog *Catalog) AddRow(inp *MdbCatalogFormat) {
	cinfo := &CollectionInfo{
		Name: inp.Ns,
		Ident: inp.Ident,
		IndexNameToInfo: make(map[string]*IndexInfo),
	}

	for idxName, idxIdent := range inp.IdxIdent {
		iinfo := &IndexInfo{
			Name: idxName,
			Ident: idxIdent,
			Owner: cinfo,
		}
		cinfo.IndexNameToInfo[idxName] = iinfo
		catalog.Indexes = append(catalog.Indexes, iinfo)
	}

	for _, index := range inp.Metadata.Indexes {
		specStr, err := bson.MarshalExtJSON(index.Spec.Key, false, false)
		if err != nil {
			panic(err)
		}
		cinfo.IndexNameToInfo[index.Spec.Name].Definition = string(specStr)
	}

	catalog.Collections = append(catalog.Collections, cinfo)
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
		FileToIndex: make(map[string]*IndexInfo),
		Collections: make([]*CollectionInfo, 0),
		Indexes: make([]*IndexInfo, 0),
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

		PPrintExt(annotateWriter, valBytes)

		var parsedFormat MdbCatalogFormat
		if err := bson.Unmarshal(valBytes, &parsedFormat); err != nil {
			panic(err)
		}

		ret.AddRow(&parsedFormat)
	}

	return ret
}

type WTList struct {
	TableToFileId map[string]int
	HexFileIdToTable map[string]string
}

var fileIdRe *regexp.Regexp = regexp.MustCompile(",id=(\\d+),")
func LoadWTList(listFile io.ReadCloser) *WTList {
	scanner := bufio.NewScanner(listFile)
	scanner.Split(bufio.ScanLines)
	defer listFile.Close()

	ret := &WTList{
		TableToFileId: make(map[string]int),
	}
	for {
		more := scanner.Scan()
		if !more {
			break
		}
		key := scanner.Text()
		if !strings.HasPrefix(key, "file:") {
			continue
		}
		tableName := key[5:len(key)-3]

		scanner.Scan()
		value := scanner.Text()
		fileIdStr := fileIdRe.FindStringSubmatch(value)[1]
		fileIdInt, err := strconv.Atoi(fileIdStr)
		if err != nil {
			panic(err)
		}

		ret.TableToFileId[tableName] = fileIdInt
	}

	return ret
}
