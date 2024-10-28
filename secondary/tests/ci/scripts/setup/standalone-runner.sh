
# this file is supposed to be a trimmed down version of dobuild
# dobuild clones gerrit patches. here the expectation is that
# all the clone has happened already in the $WORKSPACE

echo "running standalone runner - $WORKSPACE"

cd /var/www
export TS="`date +%d.%m.%Y-%H.%M`"
echo '<html><head></head><body><pre>' > /var/www/gsi-current.html
chmod a+rx /var/www/gsi-current.html

cd $WORKSPACE

# add 2ici_test flag
sed -i 's/SET (TAGS "jemalloc")/SET (TAGS "jemalloc 2ici_test")/' $WORKSPACE/goproj/src/github.com/couchbase/indexing/CMakeLists.txt

build() (
  FORCEX86=0
  if [ "$(echo $1 | tr 'A-Z' 'a-z')" = "-forcex86" ]; then
    FORCEX86=1
    echo "Forcing to build for x86 architecture"
  fi
  if [ "$BUILD_ENTERPRISE" = "" ]; then
    echo "BUILD_ENTERPRISE empty. Building enterprise edition"
    export BUILD_ENTERPRISE=true
  fi

  BUILD_ENTERPRISE=$(echo "$BUILD_ENTERPRISE" | tr 'a-z' 'A-Z')
  if [ $BUILD_ENTERPRISE = "TRUE" ]; then
    echo "Building Enterprise Edition"
  else
    echo "Building community edition"
  fi

  SUFFIX=""
  if [ $BUILD_ENTERPRISE = "TRUE" ]; then
    SUFFIX="EE"
  else
    SUFFIX="CE"
  fi

  cd $WORKSPACE
  PRODUCT_VERSION=${RELEASE}-${BLD_NUM}-rel-$SUFFIX
  rm -f *.rpm *.deb
  rm -rf ~/rpmbuild
  rm -rf $WORKSPACE/voltron/build/deb
  rm -rf $WORKSPACE/install/*
  find goproj godeps -name \*.a -print0 | xargs -0 rm -f

  cd $WORKSPACE
  mkdir -p build
  cd build

  CMAKE_x86_OPTS=
  if [ $FORCEX86 -eq 1 ];then
    CMAKE_x86_OPTS="-D CMAKE_APPLE_SILICON_PROCESSOR=x86_64 -D CMAKE_OSX_ARCHITECTURES=x86_64"
  fi

  echo "Building cmakefiles and deps [$SUFFIX]"
  cmake  ${CMAKE_x86_OPTS} \
        -D CMAKE_INSTALL_PREFIX=$WORKSPACE/install \
        -D CMAKE_PREFIX_PATH=$WORKSPACE/install \
        -D CMAKE_BUILD_TYPE=RelWithDebInfo \
        -D PRODUCT_VERSION=${PRODUCT_VERSION} \
        -D BUILD_ENTERPRISE=${BUILD_ENTERPRISE} \
        -D CB_DOWNLOAD_DEPS=1 \
        -D SNAPPY_OPTION=Disable \
        .. 1>>$WORKSPACE/make.log 2>&1
  test $? -eq 0 || error_exit "CMake build [$SUFFIX]"

  echo "Building main product [$SUFFIX]"
  cd $WORKSPACE/build
  make -j8 install 1>> $WORKSPACE/make.log 2>&1
  test $? -eq 0 || error_exit "Making Install [$SUFFIX]"

  cd $WORKSPACE
  repo manifest -r > current.xml
  repo manifest -r > manifest.xml

  echo "Actual Versions:" >> $WORKSPACE/versions.cfg
  cd $WORKSPACE
  repo forall -c 'echo "$REPO_PROJECT `git log --oneline HEAD...HEAD^`"' 2>/dev/null 1>>$WORKSPACE/versions.cfg

  chmod a+r /var/www/*
  echo "Build $SUFFIX finished"

)

build
test $? -eq 0 || exit $?

dotest 1>>/var/www/gsi-current.html 2>&1; rc=$?
echo '</pre>' >> /var/www/gsi-current.html

if [ $rc -eq 0 ]; then status=pass; else status=fail; fi
echo '<pre>' >> /var/www/gsi-current.html
gzip ${WORKSPACE}/logs.tar 2>&1 1>/dev/null
echo "Version: <a href='versions-$TS.cfg'>versions-$TS.cfg</a>" >> /var/www/gsi-current.html
echo "Build Log: <a href='make-$TS.log'>make-$TS.log</a>" >> /var/www/gsi-current.html
echo "Server Log: <a href='logs-$TS.tar.gz'>logs-$TS.tar.gz</a>" >> /var/www/gsi-current.html
echo "</pre><h1>Finished</h1></body></html>" >> /var/www/gsi-current.html
cp  /var/www/gsi-current.html /var/www/gsi-$TS.$status.html
mv ${WORKSPACE}/make.log /var/www/make-$TS.log
mv ${WORKSPACE}/logs.tar.gz /var/www/logs-$TS.tar.gz
mv ${WORKSPACE}/versions.cfg /var/www/versions-$TS.cfg
