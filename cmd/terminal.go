package main

import (
	"bfserver/machinery"
	"fmt"
	"time"
)

func downloadAndRunServerShell() {
	taskName := "mongodb_mongo_master_linux_64_duroff_required_burn_in:noPassthrough_0_linux_64_duroff_required_patch_56860f4279f56678f8460395e5d93175f4cf6546_618431960305b97f318e38b6_21_11_04_19_16_52"
	dbpath := machinery.FetchArtifactsForTask(taskName, "./tmp/")[2]

	server := machinery.NewServer(27116, dbpath, "tmp/mongod.log")
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

func runShell() {
	machinery.SpawnShell(27017)
}

func main() {
	runShell()
}
