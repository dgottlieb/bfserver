./cmd/ contains Go `main` package programs for running components:
- server.go Runs a REST API server for handling user requests
- terminal.go is an abandoned program for managing a `mongo` shell connection to a backend mongod.

./machinery/ contains the Go files running underlying service.
- fetch.go downloads and unarchives files/artifacts for evergreen tasks
- mongod.go can manage a `mongod` process.
- wt.go shells out to the `wt` cli program for dumping WT's WAL along with catalog information for mapping writes back
  to collections and indexes.
