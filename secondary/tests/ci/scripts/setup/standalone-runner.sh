#!/bin/bash

# this file is supposed to be a trimmed down version of dobuild
# dobuild clones gerrit patches. here the expectation is that
# all the clone has happened already in the $WORKSPACE

source $HOME/.cienv

echo "running standalone runner - $WORKSPACE"

export TS="$(date +%d.%m.%Y-%H.%M)"
echo '<html><head></head><body><pre>'

cd $WORKSPACE

# add 2ici_test flag
sed -i 's/SET (TAGS "jemalloc")/SET (TAGS "jemalloc 2ici_test")/' $WORKSPACE/goproj/src/github.com/couchbase/indexing/CMakeLists.txt

builder
test $? -eq 0 || exit 2

dotest
rc=$?
echo '</pre>'

if [ $rc -eq 0 ]; then status=pass; else status=fail; fi
echo '<pre>'
gzip ${WORKSPACE}/logs.tar 2>&1 1>/dev/null
echo "Version: <a href='versions-$TS.cfg'>versions-$TS.cfg</a>"
echo "Build Log: <a href='make-$TS.log'>make-$TS.log</a>"
echo "Server Log: <a href='logs-$TS.tar.gz'>logs-$TS.tar.gz</a>"
echo "</pre><h1>Finished</h1></body></html>"
