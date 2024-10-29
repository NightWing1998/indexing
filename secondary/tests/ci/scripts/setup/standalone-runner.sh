#!/bin/bash

# this file is supposed to be a trimmed down version of dobuild
# dobuild clones gerrit patches. here the expectation is that
# all the clone has happened already in the $WORKSPACE

echo "sourcing $HOME/.cienv"
source $HOME/.cienv

echo "running standalone runner - $WORKSPACE"

echo "dbg what is there is indexing?"
ls $WORKSPACE/goproj/src/github.com/couchbase/indexing

cd /var/www
export TS="$(date +%d.%m.%Y-%H.%M)"
echo '<html><head></head><body><pre>' >/var/www/gsi-current.html
chmod a+rx /var/www/gsi-current.html

cd $WORKSPACE

# add 2ici_test flag
sed -i 's/SET (TAGS "jemalloc")/SET (TAGS "jemalloc 2ici_test")/' $WORKSPACE/goproj/src/github.com/couchbase/indexing/CMakeLists.txt

builder

dotest 1>>/var/www/gsi-current.html 2>&1
rc=$?
echo '</pre>' >>/var/www/gsi-current.html

if [ $rc -eq 0 ]; then status=pass; else status=fail; fi
echo '<pre>' >>/var/www/gsi-current.html
gzip ${WORKSPACE}/logs.tar 2>&1 1>/dev/null
echo "Version: <a href='versions-$TS.cfg'>versions-$TS.cfg</a>" >>/var/www/gsi-current.html
echo "Build Log: <a href='make-$TS.log'>make-$TS.log</a>" >>/var/www/gsi-current.html
echo "Server Log: <a href='logs-$TS.tar.gz'>logs-$TS.tar.gz</a>" >>/var/www/gsi-current.html
echo "</pre><h1>Finished</h1></body></html>" >>/var/www/gsi-current.html
cp /var/www/gsi-current.html /var/www/gsi-$TS.$status.html
mv ${WORKSPACE}/make.log /var/www/make-$TS.log
mv ${WORKSPACE}/logs.tar.gz /var/www/logs-$TS.tar.gz
mv ${WORKSPACE}/versions.cfg /var/www/versions-$TS.cfg
