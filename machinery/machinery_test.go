package machinery

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestStartServer(tst *testing.T) {
	tst.SkipNow()

	server := NewServer(27116, "./testfiles/", "mongod.log")
	if err := server.StartAndWaitForListening(5 * time.Second); err != nil {
		panic(err)
	}
	server.WaitForListening(5 * time.Second)
	fmt.Println(server.Execute("test", "show tables"))
	fmt.Println(server.Execute("test", "db.bla.find()"))
	fmt.Println(server.Execute("test", "db.runCommand('dbhash')"))

	fmt.Println(server.Execute("foo", "show tables"))
	fmt.Println(server.Execute("foo", "db.bar.find()"))
	fmt.Println(server.Execute("foo", "db.runCommand('dbhash')"))
	time.Sleep(time.Second)
	server.SigInt()
	server.WaitAndPrint()
}

func TestFetchArtifacts(tst *testing.T) {
	tst.SkipNow()

	taskName := "mongodb_mongo_master_enterprise_rhel_80_64_bit_dynamic_required_noPassthrough_2_enterprise_f98b3361fbab4e02683325cc0e6ebaa69d6af1df_22_07_22_11_24_37"
	FetchArtifactsForTask(taskName, "./tmp/")
}

func TestFetchArtifactsWithTerminalShell(tst *testing.T) {
	tst.SkipNow()

	taskName := "mongodb_mongo_master_enterprise_rhel_80_64_bit_dynamic_required_noPassthrough_2_enterprise_f98b3361fbab4e02683325cc0e6ebaa69d6af1df_22_07_22_11_24_37"
	dbpath := FetchArtifactsForTask(taskName, "./tmp/")[2]

	server := NewServer(27116, dbpath, "tmp/mongod.log")
	if err := server.StartAndWaitForListening(5 * time.Second); err != nil {
		panic(err)
	}
	server.WaitForListening(5 * time.Second)
	fmt.Println("Spawning shell")
	err := server.SpawnShell()
	if err != nil {
		panic(err)
	}
	server.SigInt()
	server.WaitAndPrint()
}

func assertEquals(tst *testing.T, expected, actual interface{}) {
	if expected == actual {
		return
	}

	tst.Fatalf("Expected != Actual.\n\tExpected: `%v`\n\tActual:   `%v`", expected, actual)
}

func TestGetTaskFromUrl(tst *testing.T) {
	task := "mongodb_mongo_master_enterprise_rhel_80_64_bit_dynamic_all_feature_flags_required_concurrency_simultaneous_4_linux_enterprise_patch_9c65140283c3f72330a94e58bd9ac2c5bd090ced_63e54b7e9ccd4e19c98bf4c6_23_02_10_19_28_57"
	url := "https://spruce.mongodb.com/task/mongodb_mongo_master_enterprise_rhel_80_64_bit_dynamic_all_feature_flags_required_concurrency_simultaneous_4_linux_enterprise_patch_9c65140283c3f72330a94e58bd9ac2c5bd090ced_63e54b7e9ccd4e19c98bf4c6_23_02_10_19_28_57/files?execution=0&sortBy=STATUS&sortDir=ASC"

	assertEquals(tst, task, GetTaskFromUrl(url))
}

func TestWT(tst *testing.T) {
	// task := "https://spruce.mongodb.com/task/mongodb_mongo_master_enterprise_rhel_80_64_bit_dynamic_all_feature_flags_required_tenant_migration_stepdown_jscore_passthrough_0_linux_enterprise_patch_9c65140283c3f72330a94e58bd9ac2c5bd090ced_63e54b7e9ccd4e19c98bf4c6_23_02_10_19_28_57/files?execution=0&sortBy=STATUS&sortDir=ASC"
	// dbpaths := FetchArtifactsForTask(GetTaskFromUrl(task), "./testfiles/")
	// fmt.Println(dbpaths)
	//
	// dbpath := dbpaths[0]
	tst.SkipNow()
	dbpath := "dbpath/data/db/job4/rs0/node0"

	wtDiag := NewWTDiagnostics("./testfiles/"+dbpath, "./testfiles/wtDiag-rs0-n0")
	wtDiagResults, err := wtDiag.Run()
	if err != nil {
		tst.Fatalf("Failed to get diagnostics. Err: %v", err)
	}

	fmt.Printf("OutputFiles: %#v\n", wtDiagResults)
}

func TestAnnotate(tst *testing.T) {
	// dbpath := "dbpath/data/db/job4/rs1/node0"
	//
	// wtDiag := NewWTDiagnostics("./testfiles/"+dbpath, "./testfiles/wtDiag-rs1-n0")
	// wtDiagResults, err := wtDiag.Run()
	// if err != nil {
	//  	tst.Fatalf("Failed to get diagnostics. Err: %v", err)
	// }
	//
	// fmt.Printf("OutputFiles: %#v\n", wtDiagResults)
	catalogFile := "testfiles/wtDiag-rs1-n0/catalog"
	reader, err := os.Open(catalogFile)
	if err != nil {
		panic(err)
	}
	annotatedCatalogFile := "testfiles/wtDiag-rs1-n0/annotated_catalog"
	writer, err := os.Create(annotatedCatalogFile)
	if err != nil {
		panic(err)
	}

	catalog := LoadCatalog(reader, writer)
	fmt.Printf("Catalog: %s\n", PPrint(catalog))

	listFile := "testfiles/wtDiag-rs1-n0/list"
	reader, err = os.Open(listFile)
	if err != nil {
		panic(err)
	}
	wtList := LoadWTList(reader)
	fmt.Printf("List: %s\n", PPrint(wtList))

	printlogFile, err := os.Open("testfiles/wtDiag-rs1-n0/printlog")
	if err != nil {
		panic(err)
	}

	annotatedPrintlogFile, err := os.Create("testfiles/wtDiag-rs1-n0/annotated_printlog")
	if err != nil {
		panic(err)
	}

	RewritePrintlog(printlogFile, annotatedPrintlogFile, catalog, wtList)
}
