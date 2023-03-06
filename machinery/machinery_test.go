package machinery

import (
	"fmt"
	"testing"
	"time"
)

func TestStartServer(tst *testing.T) {
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
	taskName := "mongodb_mongo_master_enterprise_rhel_80_64_bit_dynamic_required_noPassthrough_2_enterprise_f98b3361fbab4e02683325cc0e6ebaa69d6af1df_22_07_22_11_24_37"
	FetchArtifactsForTask(taskName, "./tmp/")
}

func TestFetchArtifactsWithTerminalShell(tst *testing.T) {
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
