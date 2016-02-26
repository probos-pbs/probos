export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export PBS_DEFAULT=`cat $PROBOS_CONF_DIR/probos-controller.properties | cut -f1 -d: | head -1`
