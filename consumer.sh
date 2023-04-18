#!/usr/bin/env bash

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

echo "Writing evergreen.yml"

cat > $HOME/.evergreen.yml <<END_OF_EVG
api_server_host: https://evergreen.mongodb.com/api
ui_server_host: https://evergreen.mongodb.com
user: $EVG_USER
api_key: $EVG_API_KEY
END_OF_EVG

cat $HOME/.evergreen.yml
./bfserver --cacheDir=./cache/
