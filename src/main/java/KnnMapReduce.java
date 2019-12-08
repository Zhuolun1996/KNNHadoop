import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;

public class KnnMapReduce {

    public static final Configuration knnConf = new Configuration();

    public static void main(String[] args) throws Exception {
        knnConf.set("size", "100");
        knnConf.set("grain", "4");
        knnConf.set("k", args[1]);
        MapReduce1.run(args);
        CellMerger cellMerger = new CellMerger();
        HashMap<Integer, HashMap<String, Integer>> cellShape = cellMerger.mergeCell("knnData/output1/part-r-00000");
        knnConf.set("cellShape", Util.serializeHashMap(cellShape));
        MapReduce2.run(args);
        MapReduce3.run(args);
        MapReduce4.run(args);
    }
}


