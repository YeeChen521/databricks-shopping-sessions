#!/bin/sh
# Run a command inside the Linux dev container with the repo mounted at /w.
exec env MSYS_NO_PATHCONV=1 docker run --rm -i -v "E:/Databricks_ShoppingSessions:/w" -v ssdata:/vol ss-dev sh -c "$*"
