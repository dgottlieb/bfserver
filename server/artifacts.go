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

type ArtifactPath struct {
	PhysicalPath string
	LogicalPath  string
}

// The LogicalPath will be relative to the `taskState.DownloadDir.PhysicalPath`.
func (taskState *TaskState) GetArtifactPath(taskRelativePath string) ArtifactPath {
	absolutePath, err := filepath.Abs(taskState.DownloadDir + "/" + taskRelativePath)
	if err != nil {
		panic(err)
	}

	return taskState.GetArtifactPathFromSystemPath(absolutePath)
}

// The LogicalPath will be relative to the `taskState.DownloadDir.PhysicalPath`.
func (taskState *TaskState) GetArtifactPathFromSystemPath(systemPath string) ArtifactPath {
	// A system path may be absolute or relative to the CWD.
	absolutePath, err := filepath.Abs(systemPath)
	if err != nil {
		panic(err)
	}

	// fmt.Printf("Cutting.\n\tsystemPath: %v\n\tabsolutePath: %v\n\tDownload: %v\n",
	//  	systemPath, absolutePath, taskState.DownloadDir)
	_, logicalPath, found := strings.Cut(absolutePath, taskState.DownloadDir)
	if !found {
		panic(fmt.Sprintf("Cut not found. SystemPath: %v AbsPath: %v TaskDir: %+v",
			systemPath, absolutePath, taskState.DownloadDir))
	}

	return ArtifactPath{absolutePath, logicalPath}
}

func (taskState *TaskState) FindArtifactPath(logicalDBPath string) (ArtifactPath, error) {
	for _, dbinfo := range taskState.DBInfo {
		if dbinfo.DBPath.LogicalPath == logicalDBPath {
			return dbinfo.DBPath, nil
		}
	}

	return ArtifactPath{}, fmt.Errorf("Unknown dbpath. logicalDBPath: %v", logicalDBPath)
}

// The LogicalPath will be relative to the `artifacts.absolutePath`.
func (artifacts *Artifacts) FromPhysicalPath(systemPath string) ArtifactPath {
	// A system path may be absolute or relative to the CWD.
	absolutePath, err := filepath.Abs(systemPath)
	if err != nil {
		panic(err)
	}

	_, logicalPath, found := strings.Cut(absolutePath, artifacts.absolutePath)
	if !found {
		panic(fmt.Sprintf("Cut not found. SystemPath: %v AbsPath: %v ArtifactsDir: %+v", systemPath, absolutePath, artifacts.absolutePath))
	}

	return ArtifactPath{absolutePath, logicalPath}
}

func (artifacts *Artifacts) FromLogicalPath(logicalPath string) ArtifactPath {
	relativePath := fmt.Sprintf("%s/%s", artifacts.absolutePath, logicalPath)
	absolutePath, err := filepath.Abs(relativePath)
	if err != nil {
		panic(err)
	}

	return ArtifactPath{absolutePath, logicalPath}
}

type DBInfo struct {
	DBPath     ArtifactPath
	WtDiagPath ArtifactPath
}

type TaskState struct {
	Name   string
	DBInfo []DBInfo
	// DownloadDir ArtifactPath
	DownloadDir string
}

func CreateNewManifestFile(downloadDir, taskName string, dbpaths []string) error {
	manifestFile, err := os.Create(downloadDir + "MANIFEST")
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

func CreateManifestFile(taskState *TaskState) error {
	manifestFile, err := os.Create(taskState.DownloadDir + "MANIFEST")
	if err != nil {
		return err
	}
	defer manifestFile.Close()

	manifestFile.WriteString(taskState.Name)
	manifestFile.WriteString("\n")
	for _, dbinfo := range taskState.DBInfo {
		manifestFile.WriteString(dbinfo.DBPath.LogicalPath)
		if len(dbinfo.WtDiagPath.LogicalPath) > 0 {
			manifestFile.WriteString(" ")
			manifestFile.WriteString(dbinfo.WtDiagPath.LogicalPath)
		}
		manifestFile.WriteString("\n")
	}

	return nil
}

func (artifacts *Artifacts) LoadManifestFile(manifestPath string) (*TaskState, error) {
	absManifestPath, err := filepath.Abs(manifestPath)
	if err != nil {
		return nil, errors.Wrap(
			err,
			fmt.Sprintf("Unable to make absolute path from manifest path. Path: %v", manifestPath))
	}

	taskState := &TaskState{DownloadDir: filepath.Dir(absManifestPath) + "/"}
	// fmt.Printf("TaskPath: %v\n", taskState.DownloadDir)

	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		return nil, err
	}
	defer manifestFile.Close()

	scanner := bufio.NewScanner(manifestFile)
	scanner.Split(bufio.ScanLines)

	scanner.Scan()
	taskState.Name = scanner.Text()
	for scanner.Scan() {
		// The input string here may be formatted as `dbpath` or `dbpath wtDiagPath`:
		//   A string formatted as only `dbpath` should return `("dbpath", "")`.
		//   A string formatted as `dbpath wtDiagPath` should return `("dbpath", "wtDiagPath")`.
		dbpath, wtDiagPath, _ := strings.Cut(scanner.Text(), " ")
		toAdd := DBInfo{
			DBPath: taskState.GetArtifactPath(dbpath),
		}
		if len(wtDiagPath) > 0 {
			toAdd.WtDiagPath = taskState.GetArtifactPath(wtDiagPath)
		}
		taskState.DBInfo = append(taskState.DBInfo, toAdd)
	}

	return taskState, nil
}

func AddWtDiagToManifestFile(taskState *TaskState, dbpath, wtDiagPath ArtifactPath) error {
	found := false
	for idx, dbinfo := range taskState.DBInfo {
		if dbinfo.DBPath.LogicalPath == dbpath.LogicalPath {
			taskState.DBInfo[idx].WtDiagPath = wtDiagPath
			found = true
		}
	}
	if !found {
		panic(fmt.Sprintf("not found. dbpath: %s wtDiagPath: %s example path: %s", dbpath, wtDiagPath, taskState.DBInfo[0].DBPath))
	}

	if err := CreateManifestFile(taskState); err != nil {
		return errors.Wrap(err, "Failed to rewrite the manifest file")
	}

	return nil
}

func GetWtDiagPath(taskState *TaskState, dbpath ArtifactPath) string {
	for _, dbinfo := range taskState.DBInfo {
		if dbinfo.DBPath.LogicalPath == dbpath.LogicalPath && dbinfo.WtDiagPath.LogicalPath != "" {
			// return fmt.Sprintf("%s/printlog", dbinfo.WtDiagPath.PhysicalPath)
			return dbinfo.WtDiagPath.PhysicalPath + "/"
		}
	}

	return ""
}

func (taskState *TaskState) FullDBPath(dbpath string) string {
	return taskState.DownloadDir + dbpath
}

type Artifacts struct {
	absolutePath string
	tasksCache   map[string]*TaskState
}

func LoadArtifacts(artifactsDir string) (*Artifacts, error) {
	if err := os.MkdirAll(artifactsDir, 0755); err != nil {
		return nil, errors.Wrap(err, "Failed to create artifacts repository directory")
	}

	absolutePath, err := filepath.Abs(artifactsDir)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get an absolute path for the artifacts repository directory")
	}

	ret := &Artifacts{
		absolutePath: absolutePath,
		tasksCache:   make(map[string]*TaskState),
	}

	filepath.WalkDir(artifactsDir, func(path string, dir fs.DirEntry, err error) error {
		if dir.Name() != "MANIFEST" {
			return nil
		}

		taskState, err := ret.LoadManifestFile(path)
		if err != nil {
			panic(err)
		}

		// fmt.Printf("TaskState: %+v\n", taskState)
		ret.tasksCache[taskState.Name] = taskState
		return nil
	})

	fmt.Printf("Artifacts Loaded: %+v\n", ret)
	return ret, nil
}

func (artifacts *Artifacts) DownloadFromURL(taskUrl string) (*TaskState, error) {
	return artifacts.DownloadTask(machinery.GetTaskFromUrl(taskUrl))
}

func (artifacts *Artifacts) DownloadTask(taskName string) (*TaskState, error) {
	downloadDir, err := os.MkdirTemp(artifacts.absolutePath, "taskid_")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create directory for task artifacts")
	}
	if !strings.HasSuffix(downloadDir, "/") {
		downloadDir = downloadDir + "/"
	}

	dbpaths := machinery.FetchArtifactsForTask(taskName, downloadDir)
	if err = CreateNewManifestFile(downloadDir, taskName, dbpaths); err != nil {
		panic(err)
	}

	ret := &TaskState{
		Name:        taskName,
		DownloadDir: downloadDir,
	}
	dbinfos := make([]DBInfo, len(dbpaths))
	for idx, path := range dbpaths {
		// fmt.Println("\tHaveDBPath:", path)
		dbinfos[idx] = DBInfo{
			DBPath:     ret.GetArtifactPath(path),
			WtDiagPath: ArtifactPath{},
		}
	}
	ret.DBInfo = dbinfos
	// fmt.Printf("Downloaded.\n\tDownloadPath: %v\n\tDBPaths: %v\n\tDBInfos: %v\n", downloadDir, dbpaths, dbinfos)
	artifacts.tasksCache[taskName] = ret
	return ret, nil
}

func (artifacts *Artifacts) EnsureEvgArtifacts(taskName string) (*TaskState, error) {
	if taskState, exists := artifacts.tasksCache[taskName]; exists {
		return taskState, nil
	}

	return artifacts.DownloadTask(taskName)
}

func (artifacts *Artifacts) EnsureWTDiag(taskState *TaskState, dbpath ArtifactPath) (machinery.WTDiagnosticsResults, error) {
	if outputDir := GetWtDiagPath(taskState, dbpath); outputDir != "" {
		// Returns the `wtDiagPath/printlog` file.
		ret := machinery.WTDiagnosticsResults{
			OutputDir: outputDir,
			PrintlogFile: outputDir + "printlog",
			ListFile: outputDir + "list",
			CatalogFile: outputDir + "catalog",
			AnnotatedCatalogFile: outputDir + "annotated_catalog",
		}
		return ret, nil
	}

	systemWtDiagPath, err := os.MkdirTemp(taskState.DownloadDir, "wtDiag_")
	if err != nil {
		return machinery.WTDiagnosticsResults{}, errors.Wrap(err, "Failed to create a WT diagnostics directory")
	}
	if !strings.HasSuffix(systemWtDiagPath, "/") {
		systemWtDiagPath = systemWtDiagPath + "/"
	}

	wtDiagCmd := machinery.NewWTDiagnostics(dbpath.PhysicalPath, systemWtDiagPath)
	diagResults, err := wtDiagCmd.Run()
	if err != nil {
		panic(err)
	}

	// Also modifies TaskState to reflect `wtDiagDir`.
	if err := AddWtDiagToManifestFile(
		taskState, dbpath, taskState.GetArtifactPathFromSystemPath(systemWtDiagPath)); err != nil {
		panic(err)
	}

	return diagResults, nil
}

func (artifacts *Artifacts) AddHandlers(handlers *http.ServeMux) {
	handlers.HandleFunc("/", artifacts.ForwardTaskDownload)
	handlers.HandleFunc("/task_download", artifacts.HandleTaskDownload)
	handlers.HandleFunc("/task_view", artifacts.HandleTaskView)
	handlers.HandleFunc("/printlog", artifacts.HandlePrintlog)
	handlers.HandleFunc("/catalog", artifacts.HandleCatalog)
	handlers.HandleFunc("/list", artifacts.HandleList)
}

func handle404(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(404)
	if err := artifactTemplates.ExecuteTemplate(resp, "404.html", map[string]string{"url": req.URL.String()}); err != nil {
		panic(err)
	}
}

func (artifacts *Artifacts) ForwardTaskDownload(resp http.ResponseWriter, req *http.Request) {
	// fmt.Printf("Url: %s\n\tPath: %s\n\tRawPath: %s\n", req.URL, req.URL.Path, req.URL.RawPath)
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

	for _, dbinfo := range taskState.DBInfo {
		ret.DBPaths = append(ret.DBPaths, dbinfo.DBPath.LogicalPath)
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

	taskName, logicalDBPath := args["task"], args["dbpath"]
	taskState, exists := artifacts.tasksCache[taskName]
	if !exists {
		resp.Header().Add("Location", fmt.Sprintf("/task_view?task=%s", taskName))
		resp.WriteHeader(302)
		return
	}

	dbpath, err := taskState.FindArtifactPath(logicalDBPath)
	if err != nil {
		panic(err)
	}
	// fmt.Println("Found DBPath:", dbpath)

	wtDiagRes, err := artifacts.EnsureWTDiag(taskState, dbpath)
	if err != nil {
		panic(err)
	}
	// fmt.Println("Found PrintlogFileName:", printlogFilename)

	printlogFile, err := os.Open(wtDiagRes.PrintlogFile)
	if err != nil {
		panic(err)
	}
	defer printlogFile.Close()

	// TODO: Return the full file.
	io.CopyN(resp, printlogFile, 1*1000*1000)
}

func (artifacts *Artifacts) HandleCatalog(resp http.ResponseWriter, req *http.Request) {
	loadTemplates()
	args, err := GetFormValues(resp, req, "task", "dbpath")
	if err != nil {
		fmt.Println("Printlog arg parsing error:", err)
		return
	}

	taskName, logicalDBPath := args["task"], args["dbpath"]
	taskState, exists := artifacts.tasksCache[taskName]
	if !exists {
		resp.Header().Add("Location", fmt.Sprintf("/task_view?task=%s", taskName))
		resp.WriteHeader(302)
		return
	}

	dbpath, err := taskState.FindArtifactPath(logicalDBPath)
	if err != nil {
		panic(err)
	}
	// fmt.Println("Found DBPath:", dbpath)

	wtDiagRes, err := artifacts.EnsureWTDiag(taskState, dbpath)
	if err != nil {
		panic(err)
	}

	fmt.Println(wtDiagRes.CatalogFile)
	fmt.Println(wtDiagRes.AnnotatedCatalogFile)
	catalogFile, err := os.Open(wtDiagRes.AnnotatedCatalogFile)
	if err != nil {
		panic(err)
	}
	defer catalogFile.Close()

	// TODO: Return the full file.
	io.CopyN(resp, catalogFile, 1*1000*1000)
}

func (artifacts *Artifacts) HandleList(resp http.ResponseWriter, req *http.Request) {
	loadTemplates()
	args, err := GetFormValues(resp, req, "task", "dbpath")
	if err != nil {
		fmt.Println("Printlog arg parsing error:", err)
		return
	}

	taskName, logicalDBPath := args["task"], args["dbpath"]
	taskState, exists := artifacts.tasksCache[taskName]
	if !exists {
		resp.Header().Add("Location", fmt.Sprintf("/task_view?task=%s", taskName))
		resp.WriteHeader(302)
		return
	}

	dbpath, err := taskState.FindArtifactPath(logicalDBPath)
	if err != nil {
		panic(err)
	}
	// fmt.Println("Found DBPath:", dbpath)

	wtDiagRes, err := artifacts.EnsureWTDiag(taskState, dbpath)
	if err != nil {
		panic(err)
	}

	listFile, err := os.Open(wtDiagRes.ListFile)
	if err != nil {
		panic(err)
	}
	defer listFile.Close()

	// TODO: Return the full file.
	io.CopyN(resp, listFile, 1*1000*1000)
}
