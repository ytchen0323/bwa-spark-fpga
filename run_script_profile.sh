SPARK_DRIVER_MEMORY=28g \
  /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
  --executor-memory 48g \
  --class cs.ucla.edu.bwaspark.BWAMEMSpark \
  --total-executor-cores 1 \
  --master spark://10.0.1.2:7077 \
  --driver-java-options "-XX:+PrintFlagsFinal" \
  /home/ytchen/incubator/bwa-spark-fpga/target/bwa-spark-0.3.1-assembly.jar

SPARK_DRIVER_MEMORY=28g \
  /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
  --executor-memory 48g \
  --class cs.ucla.edu.bwaspark.BWAMEMSpark \
  --total-executor-cores 2 \
  --master spark://10.0.1.2:7077 \
  --driver-java-options "-XX:+PrintFlagsFinal" \
  /home/ytchen/incubator/bwa-spark-fpga/target/bwa-spark-0.3.1-assembly.jar

SPARK_DRIVER_MEMORY=28g \
  /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
  --executor-memory 48g \
  --class cs.ucla.edu.bwaspark.BWAMEMSpark \
  --total-executor-cores 4 \
  --master spark://10.0.1.2:7077 \
  --driver-java-options "-XX:+PrintFlagsFinal" \
  /home/ytchen/incubator/bwa-spark-fpga/target/bwa-spark-0.3.1-assembly.jar

SPARK_DRIVER_MEMORY=28g \
  /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
  --executor-memory 48g \
  --class cs.ucla.edu.bwaspark.BWAMEMSpark \
  --total-executor-cores 8 \
  --master spark://10.0.1.2:7077 \
  --driver-java-options "-XX:+PrintFlagsFinal" \
  /home/ytchen/incubator/bwa-spark-fpga/target/bwa-spark-0.3.1-assembly.jar

SPARK_DRIVER_MEMORY=28g \
  /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
  --executor-memory 48g \
  --class cs.ucla.edu.bwaspark.BWAMEMSpark \
  --total-executor-cores 16 \
  --master spark://10.0.1.2:7077 \
  --driver-java-options "-XX:+PrintFlagsFinal" \
  /home/ytchen/incubator/bwa-spark-fpga/target/bwa-spark-0.3.1-assembly.jar

SPARK_DRIVER_MEMORY=28g \
  /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
  --executor-memory 48g \
  --class cs.ucla.edu.bwaspark.BWAMEMSpark \
  --total-executor-cores 24 \
  --master spark://10.0.1.2:7077 \
  --driver-java-options "-XX:+PrintFlagsFinal" \
  /home/ytchen/incubator/bwa-spark-fpga/target/bwa-spark-0.3.1-assembly.jar
