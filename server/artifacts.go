package server

import (
	"bufio"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"bfserver/machinery"
)

var artifactTemplates *template.Template

func loadTemplates() {
	var err error
	if artifactTemplates, err = template.ParseFiles(
		"../server/templates/task_download.html",
		"../server/templates/404.html",
		"../server/templates/task_view.html",
		// "../server/templates/printlog.html",
	); err != nil {
		panic(err)
	}
}

func init() {
	loadTemplates()
}

type TaskState struct {
	Name        string
	DBPaths     []string
	DownloadDir string
}

func CreateManifestFile(downloadDir, taskName string, dbpaths []string) error {
	manifestFile, err := os.Create(fmt.Sprintf("%s/MANIFEST", downloadDir))
	if err != nil {
		return err
	}
	defer manifestFile.Close()

	manifestFile.WriteString(taskName)
	manifestFile.WriteString("\n")
	for _, dbpath := range dbpaths {
		manifestFile.WriteString(dbpath)
		manifestFile.WriteString("\n")
	}

	return nil
}

func LoadManifestFile(walkDirPath string, manifestDirEntry fs.DirEntry) (*TaskState, error) {
	ret := &TaskState{DownloadDir: filepath.Dir(walkDirPath)}

	manifestFile, err := os.Open(walkDirPath)
	if err != nil {
		return nil, err
	}
	defer manifestFile.Close()

	scanner := bufio.NewScanner(manifestFile)
	scanner.Split(bufio.ScanLines)

	scanner.Scan()
	ret.Name = scanner.Text()
	for scanner.Scan() {
		ret.DBPaths = append(ret.DBPaths, scanner.Text())
	}

	return ret, nil
}

func (taskState *TaskState) FullDBPath(dbpath string) string {
	return fmt.Sprintf("%s/%s", taskState.DownloadDir, dbpath)
}

type Artifacts struct {
	dir        string
	tasksCache map[string]*TaskState
}

func LoadArtifacts(artifactsDir string) (*Artifacts, error) {
	if err := os.MkdirAll(artifactsDir, 0755); err != nil {
		return nil, errors.Wrap(err, "Failed to create artifacts repository directory")
	}

	taskCache := make(map[string]*TaskState)
	filepath.WalkDir(artifactsDir, func(path string, dir fs.DirEntry, err error) error {
		if dir.Name() != "MANIFEST" {
			return nil
		}

		taskState, err := LoadManifestFile(path, dir)
		if err != nil {
			panic(err)
		}

		fmt.Printf("TaskState: %+v\n", taskState)
		taskCache[taskState.Name] = taskState
		return nil
	})

	return &Artifacts{artifactsDir, taskCache}, nil
}

func (artifacts *Artifacts) DownloadFromURL(taskUrl string) (*TaskState, error) {
	return artifacts.DownloadTask(machinery.GetTaskFromUrl(taskUrl))
}

func (artifacts *Artifacts) DownloadTask(taskName string) (*TaskState, error) {
	downloadDir, err := os.MkdirTemp(artifacts.dir, "taskid_")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create directory for task artifacts")
	}

	dbpaths := machinery.FetchArtifactsForTask(taskName, fmt.Sprintf("%s/", downloadDir))
	if err = CreateManifestFile(downloadDir, taskName, dbpaths); err != nil {
		panic(err)
	}
	fmt.Println("Downloaded. DBPaths:", dbpaths)
	ret := &TaskState{taskName, dbpaths, downloadDir}
	artifacts.tasksCache[taskName] = ret
	return ret, nil
}

func (artifacts *Artifacts) EnsureEvgArtifacts(taskName string) (*TaskState, error) {
	if taskState, exists := artifacts.tasksCache[taskName]; exists {
		return taskState, nil
	}

	return artifacts.DownloadTask(taskName)
}

func (artifacts *Artifacts) EnsureWTDiag(taskState *TaskState, dbpath string) (string, error) {
	fullPath := taskState.FullDBPath(dbpath)
	wtDiagCmd := machinery.NewWTDiagnostics(fullPath, taskState.DownloadDir)
	diagResults, err := wtDiagCmd.Run()
	if err != nil {
		panic(err)
	}

	return diagResults.PrintlogFile, nil
}

func (artifacts *Artifacts) AddHandlers(handlers *http.ServeMux) {
	handlers.HandleFunc("/", artifacts.ForwardTaskDownload)
	handlers.HandleFunc("/task_download", artifacts.HandleTaskDownload)
	handlers.HandleFunc("/task_view", artifacts.HandleTaskView)
	handlers.HandleFunc("/printlog", artifacts.HandlePrintlog)
}

func handle404(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(404)
	if err := artifactTemplates.ExecuteTemplate(resp, "404.html", map[string]string{"url": req.URL.String()}); err != nil {
		panic(err)
	}
}

func (artifacts *Artifacts) ForwardTaskDownload(resp http.ResponseWriter, req *http.Request) {
	fmt.Printf("Url: %s\n\tPath: %s\n\tRawPath: %s\n", req.URL, req.URL.Path, req.URL.RawPath)
	if req.URL.Path == "/" {
		resp.Header().Add("Location", "/task_download")
		resp.WriteHeader(302)
		return
	}
	handle404(resp, req)
}

func (artifacts *Artifacts) HandleTaskDownload(resp http.ResponseWriter, req *http.Request) {
	loadTemplates()
	if err := artifactTemplates.ExecuteTemplate(resp, "task_download.html", nil); err != nil {
		panic(err)
	}
}

type TaskViewArgs struct {
	Name    string
	DBPaths []string
}

func NewTaskViewArgs(taskState *TaskState) *TaskViewArgs {
	ret := &TaskViewArgs{
		Name: taskState.Name,
	}

	for _, dbpath := range taskState.DBPaths {
		ret.DBPaths = append(ret.DBPaths, dbpath[len(taskState.DownloadDir)+1:])
	}

	return ret
}

func (artifacts *Artifacts) HandleTaskView(resp http.ResponseWriter, req *http.Request) {
	loadTemplates()
	req.ParseForm()
	taskNameForm, exists := req.Form["task"]
	if !exists {
		handle404(resp, req)
		return
	}
	taskName := taskNameForm[0]

	if strings.HasPrefix(taskName, "http://") || strings.HasPrefix(taskName, "https://") {
		taskName = machinery.GetTaskFromUrl(taskName)
	}

	taskState, err := artifacts.EnsureEvgArtifacts(taskName)
	_ = taskState
	if err != nil {
		panic(err)
	}

	if err := artifactTemplates.ExecuteTemplate(resp, "task_view.html", NewTaskViewArgs(taskState)); err != nil {
		panic(err)
	}
}

func GetFormValues(resp http.ResponseWriter, req *http.Request, keys ...string) (map[string]string, error) {
	req.ParseForm()

	ret := make(map[string]string)
	for _, key := range keys {
		if val, exists := req.Form[key]; exists {
			ret[key] = val[0]
		} else {
			handle404(resp, req)
			return nil, fmt.Errorf("Did not find key: %s", key)
		}
	}

	return ret, nil
}

func (artifacts *Artifacts) HandlePrintlog(resp http.ResponseWriter, req *http.Request) {
	loadTemplates()
	args, err := GetFormValues(resp, req, "task", "dbpath")
	if err != nil {
		fmt.Println("Printlog arg parsing error:", err)
		return
	}

	taskName, dbpath := args["task"], args["dbpath"]
	taskState, exists := artifacts.tasksCache[taskName]
	if !exists {
		resp.Header().Add("Location", fmt.Sprintf("/task_view?task=%s", taskName))
		resp.WriteHeader(302)
		return
	}

	printlogFilename, err := artifacts.EnsureWTDiag(taskState, dbpath)
	if err != nil {
		panic(err)
	}

	printlogFile, err := os.Open(printlogFilename)
	if err != nil {
		panic(err)
	}
	defer printlogFile.Close()

	// TODO: Return the full file.
	io.CopyN(resp, printlogFile, 10*1000*1000)
}
