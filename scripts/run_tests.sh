BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $BASE_DIR
echo "$BASE_DIR"
bash -c "while true; do echo -n .; sleep 5; done" &
PROGRESS_REPORTER_PID=$!
time ./sbt ++$TRAVIS_SCALA_VERSION $TEST_TARGET/compile $TEST_TARGET/test:compile &> /dev/null
kill -9 $PROGRESS_REPORTER_PID

export JVM_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:ReservedCodeCacheSize=96m -XX:+TieredCompilation -XX:MaxPermSize=128m -Xms512m -Xmx2048m -Xss2m"

./sbt -Dlog4j.configuration=file://$TRAVIS_BUILD_DIR/project/travis-log4j.properties ++$TRAVIS_SCALA_VERSION $TEST_TARGET/test