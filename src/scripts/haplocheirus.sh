#!/bin/sh
#
# haplocheirus init.d script.
#
# All java services require the same directory structure:
#   /usr/local/$APP_NAME-$VERSION
#   /var/log/$APP_NAME (chown daemon, chmod 775)

APP_NAME="haplocheirus"
ADMIN_PORT="7667"
VERSION="@VERSION@"
APP_HOME="/usr/local/$APP_NAME/current"
AS_USER="haplo"
DAEMON="/usr/local/bin/daemon"

HEAP_OPTS="-Xmx4096m -Xms4096m -XX:NewSize=768m"
JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
GC_OPTS="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
GC_LOG="-Xloggc:/var/log/$APP_NAME/gc.log"
DEBUG_OPTS="-XX:ErrorFile=/var/log/$APP_NAME/java_error%p.log"
JAVA_OPTS="-server $GC_OPTS $GC_LOG $HEAP_OPTS $JMX_OPTS $DEBUG_OPTS"

pidfile="/var/run/$APP_NAME/$APP_NAME.pid"
daemon_args="--name $APP_NAME --pidfile $pidfile --core --chdir /"
daemon_start_args="--user $AS_USER --stdout=/var/log/$APP_NAME/stdout --stderr=/var/log/$APP_NAME/error"

function running() {
  $DAEMON $daemon_args --running
}

function find_java() {
  if [ ! -z "$JAVA_HOME" ]; then
    return
  fi
  for dir in /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default; do
    if [ -x $dir/bin/java ]; then
      JAVA_HOME=$dir
      break
    fi
  done
}

# dirs under /var/run can go away between reboots.
for p in /var/run/$APP_NAME /var/log/$APP_NAME; do
  if [ ! -d $p ]; then
    mkdir -p $p
    chmod 775 $p
    chown $AS_USER $p >/dev/null 2>&1 || true
  fi
done

find_java


case "$1" in
  start)
    echo -n "Starting $APP_NAME... "

    if [ ! -r $APP_HOME/$APP_NAME-$VERSION.jar ]; then
      echo "FAIL"
      echo "*** $APP_NAME jar missing: $APP_HOME/$APP_NAME-$VERSION.jar - not starting"
      exit 1
    fi
    if [ ! -x $JAVA_HOME/bin/java ]; then
      echo "FAIL"
      echo "*** $JAVA_HOME/bin/java doesn't exist -- check JAVA_HOME?"
      exit 1
    fi
    if running; then
      echo "already running."
      exit 0
    fi
    
    ulimit -n 32768 || echo -n " (no ulimit)"
    ulimit -c unlimited || echo -n " (no coredump)"
    $DAEMON $daemon_args $daemon_start_args -- ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar ${APP_HOME}/${APP_NAME}-${VERSION}.jar
    tries=0
    while ! running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;

  stop)
    echo -n "Stopping $APP_NAME... "
    if ! running; then
      echo "wasn't running."
      exit 0
    fi

    curl -s http://localhost:${ADMIN_PORT}/shutdown.txt
    tries=0
    while running; do
      tries=$((tries + 1))
      if [ $tries -ge 15 ]; then
        echo "FAILED SOFT SHUTDOWN, TRYING HARDER"
        if [ -f $pidfile ]; then
          kill $(cat $pidfile)
        else
          echo "CAN'T FIND PID, TRY KILL MANUALLY"
          exit 1
        fi
        hardtries=0
        while running; do
          hardtries=$((hardtries + 1))
          if [ $hardtries -ge 5 ]; then
            echo "FAILED HARD SHUTDOWN, TRY KILL -9 MANUALLY"
            exit 1
          fi
          sleep 1
        done
      fi
      sleep 1
    done
    echo "done."
  ;;
  
  status)
    if running; then
      echo "$APP_NAME is running."
    else
      echo "$APP_NAME is NOT running."
    fi
  ;;

  restart)
    $0 stop
    sleep 2
    $0 start
  ;;

  *)
    echo "Usage: /etc/init.d/${APP_NAME}.sh {start|stop|restart|status}"
    exit 1
  ;;
esac

exit 0
