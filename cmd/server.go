package main

import (
	"net/http"
	"time"

	"bfserver/server"
)

func main() {
	artifacts, err := server.LoadArtifacts("server_artifacts_cache")
	if err != nil {
		panic(err)
	}
	_ = artifacts
	handler := http.NewServeMux()
	artifacts.AddHandlers(handler)

	server := &http.Server{
		Addr:              "127.0.0.1:8080",
		Handler:           handler,
		ReadHeaderTimeout: time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
