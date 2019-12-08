import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;

public class KnnMapReduce {

    public static final Configuration knnConf = new Configuration();

    public static void main(String[] args) throws Exception {
        knnConf.set("size", "100");
        knnConf.set("grain", "4");
        knnConf.set("k", args[1]);
        knnConf.set("fs.defaultFS", "hdfs://ric-master-01.sci.pitt.edu:8020");
        MapReduce1.run(args);
        CellMerger cellMerger = new CellMerger();
        FileSystem hdfsFileSystem = FileSystem.get(knnConf);
        FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        FileUtil.copyMerge(hdfsFileSystem, new Path("/user/zhl137/knnData/output1/"), localFileSystem, new Path("./knnData/output1/output"), false, knnConf, null);
        HashMap<Integer, HashMap<String, Integer>> cellShape = cellMerger.mergeCell("knnData/output1/output");
        knnConf.set("cellShape", Util.serializeHashMap(cellShape));
        MapReduce2.run(args);
        MapReduce3.run(args);
        MapReduce4.run(args);
    }
}


