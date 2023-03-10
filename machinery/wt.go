package machinery

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
)

type WTDiagnostics struct {
	DBPath    string
	OutputDir string
}

func NewWTDiagnostics(dbpath string, outputDir string) *WTDiagnostics {
	if !strings.HasSuffix(outputDir, "/") {
		outputDir = outputDir + "/"
	}
	return &WTDiagnostics{dbpath, outputDir}
}

type WTDiagnosticsResults struct {
	OutputDir string

	PrintlogFile          string
	ListFile              string
	CatalogFile           string
	AnnotatedCatalogFile  string
	AnnotatedPrintlogFile string
}

func ReadStderr(stderr io.ReadCloser) string {
	buf, err := io.ReadAll(stderr)
	if err != nil {
		return fmt.Sprintf("Failed to read stderr. Err: %s", err)
	}

	return string(buf)
}

func RunCommand(cmd *exec.Cmd, outputFile string) error {
	fmt.Println("Cmd:", cmd.String())
	readCloser, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	cmdOut, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer cmdOut.Close()

	err = cmd.Start()
	if err != nil {
		return err
	}

	var stderr string
	buf := make([]byte, 10*1024)
	for {
		read, err := readCloser.Read(buf)
		if read == 0 && err == io.EOF {
			break
		}

		if err != nil {
			stderr = ReadStderr(stderrPipe)
			cmd.Wait()
			return err
		}

		_, err = cmdOut.Write(buf[:read])
		if err != nil {
			stderr = ReadStderr(stderrPipe)
			cmd.Wait()
			return err
		}
	}

	stderr = ReadStderr(stderrPipe)
	err = cmd.Wait()
	if err != nil {
		return errors.Wrap(err, stderr)
	}

	return nil
}

func (wtDiag *WTDiagnostics) Run() (WTDiagnosticsResults, error) {
	err := os.MkdirAll(wtDiag.OutputDir, 0750)
	if err != nil {
		return WTDiagnosticsResults{}, err
	}

	ret := WTDiagnosticsResults{
		OutputDir:             wtDiag.OutputDir,
		PrintlogFile:          wtDiag.OutputDir + "printlog",
		ListFile:              wtDiag.OutputDir + "list",
		CatalogFile:           wtDiag.OutputDir + "catalog",
		AnnotatedCatalogFile:  wtDiag.OutputDir + "annotated_catalog",
		AnnotatedPrintlogFile: wtDiag.OutputDir + "annotated_printlog",
	}

	printlogCmd := exec.Command(
		"wt", "-C", "log=(compressor=snappy,path=journal),verbose=()", "-h", wtDiag.DBPath, "-r",
		"printlog", "-u", "-x")
	if err := RunCommand(printlogCmd, ret.PrintlogFile); err != nil {
		return ret, errors.Wrap(err, "Failed to get the WT journal output")
	}

	listCmd := exec.Command(
		"wt", "-C", "log=(compressor=snappy,path=journal),verbose=()", "-h", wtDiag.DBPath, "-r",
		"list", "-v")
	if err := RunCommand(listCmd, ret.ListFile); err != nil {
		return ret, errors.Wrap(err, "Failed to get the WT list output")
	}

	catalogCmd := exec.Command(
		"wt", "-C", "log=(compressor=snappy,path=journal),verbose=()", "-h", wtDiag.DBPath, "-r",
		"dump", "-x", "table:_mdb_catalog")
	if err := RunCommand(catalogCmd, ret.CatalogFile); err != nil {
		return ret, errors.Wrap(err, "Failed to get the MDB catalog output")
	}

	catalogFile, err := os.Open(ret.CatalogFile)
	if err != nil {
		panic(err)
	}
	annotatedCatalogFile, err := os.Create(ret.AnnotatedCatalogFile)
	if err != nil {
		panic(err)
	}
	catalog := LoadCatalog(catalogFile, annotatedCatalogFile)

	wtListFile, err := os.Open(ret.ListFile)
	if err != nil {
		panic(err)
	}
	wtList := LoadWTList(wtListFile)

	printlogFile, err := os.Open(ret.PrintlogFile)
	if err != nil {
		panic(err)
	}
	annotatedPrintlogFile, err := os.Create(ret.AnnotatedPrintlogFile)
	if err != nil {
		panic(err)
	}
	RewritePrintlog(printlogFile, annotatedPrintlogFile, catalog, wtList)

	return ret, nil
}
