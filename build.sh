cd hadoop-hdfs-project/hadoop-hdfs/src/main/specpaxos && make finalLib -j4 && cd - && mvn package -Pdist -DskipTests -Dmaven.javadoc.skip=true -Dtar
