import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

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

public class MapReduce2 {

    private static final Logger LOG = Logger.getLogger(MapReduce2.class);

    public static void run(String[] args) throws Exception {
        Job job = Job.getInstance(KnnMapReduce.knnConf, "knnMapReduce2");
        job.setJarByClass(MapReduce2.class);
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0] + "/input"));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/output2"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }


    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        private int getCellIdByCoordinate(String coordinate, HashMap<Integer, HashMap<String, Integer>> cellShape) {
            int x = Integer.parseInt(coordinate.split(",")[1]);
            int y = Integer.parseInt(coordinate.split(",")[2]);

            for (Entry<Integer, HashMap<String, Integer>> cell : cellShape.entrySet()) {
                if ((cell.getValue().get("leftMargin") < x && cell.getValue().get("rightMargin") >= x) && (cell.getValue().get("topMargin") < y && cell.getValue().get("bottomMargin") >= y)) {
                    return cell.getKey();
                }
            }
            return -1;
        }

        @Override
        public void map(LongWritable offset, Text coordinateText, Context context)
                throws IOException, InterruptedException {
            String coordinateString = coordinateText.toString();
            try {
                HashMap<Integer, HashMap<String, Integer>> cellShape = Util.deserializeHashMap(context.getConfiguration().get("cellShape"));
                context.write(new IntWritable(getCellIdByCoordinate(coordinateString, cellShape)), new Text(coordinateString));
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
            ArrayList<PointInfo> pointInfoList = new ArrayList<>();
            for (Text pointInfoText : pointInfoIterable) {
                pointInfoList.add(new PointInfo(pointInfoText.toString(), 0));
            }
            for (PointInfo pointInfo : pointInfoList) {
                ArrayList<PointDistance> knnList = Util.getKnnList(pointInfo, pointInfoList, k);
                String value = pointInfo.getX() + ";" + pointInfo.getY() + ";" + cellId + ";";
                value += knnList.toString();
                context.write(new IntWritable(pointInfo.getPointId()), new Text(value));
            }
        }
    }
}


