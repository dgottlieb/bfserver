package machinery

import (
	"fmt"
	"io"
	"os"
	"os/exec"
)

type WTDiagnostics struct {
	DBPath    string
	OutputDir string
}

func NewWTDiagnostics(dbpath string, outputDir string) *WTDiagnostics {
	return &WTDiagnostics{dbpath, outputDir}
}

type WTDiagnosticsResults struct {
	OutputDir string

	PrintlogFile string
	ListFile     string
	CatalogFile  string
}

func (wtDiag *WTDiagnostics) Run() (WTDiagnosticsResults, error) {
	ret := WTDiagnosticsResults{OutputDir: wtDiag.OutputDir}

	printlog := exec.Command("wt", "-C", "log=(compressor=snappy,path=journal),verbose=()", "-h", wtDiag.DBPath, "-r", "printlog", "-u", "-x")
	fmt.Println("Cmd:", printlog.String())

	readCloser, err := printlog.StdoutPipe()
	if err != nil {
		return ret, err
	}

	err = os.MkdirAll(wtDiag.OutputDir, 0750)
	if err != nil {
		return ret, err
	}

	ret.PrintlogFile = wtDiag.OutputDir + "printlog"
	printlogOut, err := os.Create(ret.PrintlogFile)
	if err != nil {
		return ret, err
	}
	defer printlogOut.Close()

	err = printlog.Start()
	if err != nil {
		return ret, err
	}

	buf := make([]byte, 10*1024)
	for {
		read, err := readCloser.Read(buf)
		if read == 0 && err == io.EOF {
			break
		}

		if err != nil {
			printlog.Wait()
			return ret, err
		}

		_, err = printlogOut.Write(buf[:read])
		if err != nil {
			printlog.Wait()
			return ret, err
		}
	}

	err = printlog.Wait()
	if err != nil {
		return ret, err
	}

	return ret, nil
}
