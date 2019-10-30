
# Set JAVA HOME
export JAVA_HOME=/usr/lib/jvm/java

# Add hadoop configuration directory into classpath
export HADOOP_CONF_DIR=/usr/hdp/2.6.5.0-292/hadoop/conf


# Setup the environment for SparkGraphComputer
# Add yarn and spark lib and config into classpath for SparkGraphComputer.
export YARN_HOME=/usr/hdp/current/hadoop-yarn-client
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_CONF_DIR=/usr/hdp/current/spark2-client/conf
export JANUSGRAPH_HOME=/usr/janusgraph/0.2.2
export CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR:$SPARK_CONF_DIR:$JANUSGRAPH_HOME/conf

#add hbase configuration directory into classpath
if ([ -d "/usr/hdp/current/hbase-client/conf" ]); then
   export HBASE_CONF_DIR=/usr/hdp/current/hbase-client/conf
   export CLASSPATH=$CLASSPATH:$HBASE_CONF_DIR
fi

if ([[ ! -d "/usr/janusgraph/0.2.2//ext/spark-client/plugin" ]] && [[ -d "$SPARK_HOME/jars" ]]); then
  for jar in $SPARK_HOME/jars/*.jar; do
    if ([[ $jar != */guava*.jar ]] && [[ $jar != */slf4j-log4j12*.jar ]] && [[ $jar != */spark-core*.jar ]]) ;then
      CLASSPATH=$CLASSPATH:$jar
    fi
  done
fi

export CLASSPATH

# Add iop.version and native lib in java opt for hadoop config.
export IOP_JAVA_OPTIONS="$JAVA_OPTIONS -Dhdp.version=2.6.5.0-292 -Djava.library.path=/usr/hdp/current/hadoop-client/lib/native"



source "$HADOOP_CONF_DIR"/hadoop-env.sh
export HADOOP_GREMLIN_LIBS=$JANUSGRAPH_HOME/lib
export JANUSGRAPH_LOGDIR=/var/log/janusgraph