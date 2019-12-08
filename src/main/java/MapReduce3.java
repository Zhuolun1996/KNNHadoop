import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.log4j.Logger;

public class MapReduce3 {

    private static final Logger LOG = Logger.getLogger(MapReduce3.class);


    public static void run(String[] args) throws Exception {
        Job job = Job.getInstance(KnnMapReduce.knnConf, "knnMapReduce3");
        job.setJarByClass(MapReduce3.class);
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0] + "/output2/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/output3"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }


    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        private ArrayList<Entry<Integer, String>> getValueStringList(PointInfo pointInfo, HashMap<Integer, HashMap<String, Integer>> cellShape) {
            Float maxDistance = pointInfo.getKnnList().get(0).getDistance();
            ArrayList<Entry<Integer, String>> resultList = new ArrayList<>();
            boolean isOverlapped = false;
            for (Entry<Integer, HashMap<String, Integer>> cell : cellShape.entrySet()) {
                if ((pointInfo.getX() - maxDistance < cell.getValue().get("rightMargin") && pointInfo.getX() >= cell.getValue().get("rightMargin")) || (pointInfo.getX() + maxDistance >= cell.getValue().get("leftMargin") && pointInfo.getX() < cell.getValue().get("leftMargin"))
                        || (pointInfo.getY() - maxDistance < cell.getValue().get("bottomMargin") && pointInfo.getY() >= cell.getValue().get("bottomMargin")) || (pointInfo.getY() + maxDistance >= cell.getValue().get("topMargin") && pointInfo.getY() < cell.getValue().get("topMargin"))) {
                    resultList.add(new SimpleEntry<>(cell.getKey(), pointInfo.getPointId() + ";" + pointInfo.getX() + ";" + pointInfo.getY() + ";" + pointInfo.getCellId() + ";" + pointInfo.getKnnList().toString() + ";" + "false"));
                    isOverlapped = true;
                }
            }
            if (!isOverlapped) {
                resultList.add(new SimpleEntry<>(pointInfo.getCellId(), pointInfo.getPointId() + ";" + pointInfo.getX() + ";" + pointInfo.getY() + ";" + pointInfo.getCellId() + ";" + pointInfo.getKnnList().toString() + ";" + "true"));
            }
            return resultList;
        }

        @Override
        public void map(LongWritable offset, Text knnInfoText, Context context)
                throws IOException, InterruptedException {
            String knnInfoString = knnInfoText.toString();
            PointInfo pointInfo = new PointInfo(knnInfoString, 1);
            try {
                HashMap<Integer, HashMap<String, Integer>> cellShape = Util.deserializeHashMap(context.getConfiguration().get("cellShape"));
                for (Entry<Integer, String> value : getValueStringList(pointInfo, cellShape)) {
                    context.write(new IntWritable(value.getKey()), new Text(value.getValue()));
                }
                context.write(new IntWritable(pointInfo.getCellId()), new Text(pointInfo.getPointId() + ";" + pointInfo.getX() + ";" + pointInfo.getY()));
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable cellId, Iterable<Text> pointInfoIterable, Context context)
                throws IOException, InterruptedException {
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            ArrayList<PointInfo> rawPointList = new ArrayList<>();
            ArrayList<PointInfo> knnPointList = new ArrayList<>();
            for (Text pointInfoText : pointInfoIterable) {
                String pointInfoString = pointInfoText.toString();
                if (pointInfoString.split(";").length == 3) {
                    rawPointList.add(new PointInfo(pointInfoString, 2));
                } else {
                    knnPointList.add(new PointInfo(pointInfoString, 3));
                }
            }

            for (PointInfo pointInfo : knnPointList) {
                ArrayList<PointDistance> knnList = Util.getKnnList(pointInfo, rawPointList, k);
                pointInfo.setKnnList(knnList);
                context.write(new IntWritable(pointInfo.getPointId()), new Text(pointInfo.getX() + ";" + pointInfo.getY() + ";" + cellId.toString() + ";" + pointInfo.getKnnList().toString()));
            }

        }
    }
}


