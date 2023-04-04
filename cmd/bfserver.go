package main

import (
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"time"

	"bfserver/server"
)

func main() {
	var cacheDir *string = flag.String("cacheDir", "", "A directory where downloaded content is cached.")
	flag.Parse()
	if *cacheDir == "" {
		panic("A directory cache not passed in. Use --cacheDir.")
	}

	artifacts, err := server.LoadArtifacts(*cacheDir)
	if err != nil {
		panic(err)
	}
	_ = artifacts
	handler := http.NewServeMux()
	artifacts.AddHandlers(handler)

	for _, env := range os.Environ() {
		fmt.Println("Env:", env)
	}

	fileSystem := os.DirFS(".")
	fs.WalkDir(fileSystem, ".", func(path string, dir fs.DirEntry, err error) error {
		fmt.Println(path)
		return nil
	})

	server := &http.Server{
		Addr:              "0.0.0.0:8080",
		Handler:           handler,
		ReadHeaderTimeout: time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
