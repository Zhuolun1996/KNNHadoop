export JAVA_HOME=/usr/local/jdk1.8.0_101
rm ./knnData/output1/output
rm ./knnData/result
rm ./knnData/output*
hadoop fs -rm -r knnData/output1
hadoop fs -rm -r knnData/output2
hadoop fs -rm -r knnData/output3
hadoop fs -rm -r knnData/output4
hadoop jar knnMapReduce.jar KnnMapReduce "$@"