package server

import (
	"os"
	"testing"
)

func assertEquals(tst *testing.T, expected, actual interface{}) {
	if expected == actual {
		return
	}

	// panic(fmt.Sprintf("Expected != Actual.\n\tExpected: `%v`\n\tActual:   `%v`", expected, actual))
	tst.Fatalf("Expected != Actual.\n\tExpected: `%v`\n\tActual:   `%v`", expected, actual)
}

func TestManifestRoundtrip(tst *testing.T) {
	if err := os.RemoveAll("./testfiles/"); err != nil {
		panic(err)
	}
	// Create a `task_123` to directly write a synthetic `MANIFEST` file into.
	if err := os.MkdirAll("./testfiles/task_123", 0755); err != nil {
		panic(err)
	}

	// `testfiles` is the repository root. MANIFEST files should not be aware of the physical
	// `testfiles` location.
	artifacts, err := LoadArtifacts("./testfiles/")
	if err != nil {
		panic(err)
	}

	// This call represents the following directory structure rooted at `testfiles`:
	// - testfiles/
	//   - task_123/
	//     - MANIFEST
	//     - evg/
	//       - dbpath1/<db files>
	//       - dbpath2/<db files>
	if err := CreateNewManifestFile("./testfiles/task_123/", "taskName", []string{"evg/dbpath1", "evg/dbpath2"}); err != nil {
		panic(err)
	}

	state, err := artifacts.LoadManifestFile("./testfiles/task_123/MANIFEST")
	if err != nil {
		panic(err)
	}

	assertEquals(tst, "taskName", state.Name)
	assertEquals(tst, 2, len(state.DBInfo))
	assertEquals(tst, "evg/dbpath1", state.DBInfo[0].DBPath.LogicalPath)
	assertEquals(tst, "evg/dbpath2", state.DBInfo[1].DBPath.LogicalPath)

	// This call represents the following directory structure rooted at `testfiles`:
	// - testfiles/
	//   - task_123/
	//     - MANIFEST
	//     - evg/
	//       - dbpath1/<db files>
	//       - dbpath2/<db files>
	//     - wtDiag_456/<printlog and other diagnostic files>
	AddWtDiagToManifestFile(
		state,
		artifacts.FromLogicalPath("evg/dbpath2"),
		state.GetArtifactPathFromSystemPath("./testfiles/task_123/wtDiag_456"))
	// Calling `AddWtDiagToManifestFile` modifies the state of `state`.
	assertEquals(tst, "wtDiag_456", state.DBInfo[1].WtDiagPath.LogicalPath)

	state, err = artifacts.LoadManifestFile("./testfiles/task_123/MANIFEST")
	if err != nil {
		panic(err)
	}
	// Loading fresh sees a set `WtDiagPath`.
	assertEquals(tst, ArtifactPath{}, state.DBInfo[0].WtDiagPath)
	assertEquals(tst, "wtDiag_456", state.DBInfo[1].WtDiagPath.LogicalPath)
}
