import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
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

public class MapReduce4 {

    private static final Logger LOG = Logger.getLogger(MapReduce4.class);
    private static Configuration knnConf;
    private static Integer size;
    private static Integer grain;
    private static Integer k;
    private static HashMap<Integer, HashMap<String, Integer>> cellShape;

    public static void run(String[] args, Configuration knnConf, HashMap<Integer, HashMap<String, Integer>> shape) throws Exception {
        MapReduce4.knnConf = knnConf;
        size = Integer.parseInt(knnConf.get("size"));
        grain = Integer.parseInt(knnConf.get("grain"));
        cellShape = shape;
        k = Integer.parseInt(knnConf.get("k"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "knnMapReduce4");
        job.setJarByClass(MapReduce4.class);
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0] + "/output3/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/output4"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }


    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable offset, Text knnInfoText, Context context)
                throws IOException, InterruptedException {
            String knnInfoString = knnInfoText.toString();
            PointInfo pointInfo = new PointInfo(knnInfoString, 1);

            context.write(new IntWritable(pointInfo.getPointId()), new Text(pointInfo.getKnnList().toString()));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable pointId, Iterable<Text> knnListTextIterable, Context context)
                throws IOException, InterruptedException {
            ArrayList<PointDistance> knnList = new ArrayList<>();
            PriorityQueue<PointDistance> pointDistanceQueue = new PriorityQueue<>(new Util.PointDistanceComparator());
            for (Text knnListText : knnListTextIterable) {
                String knnListString = knnListText.toString();
                pointDistanceQueue.addAll(Util.knnListStringToKnnList(knnListString));
            }
            while (pointDistanceQueue.size() > k) {
                pointDistanceQueue.poll();
            }
            while (!pointDistanceQueue.isEmpty()) {
                PointDistance pointDistance = pointDistanceQueue.poll();
                knnList.add(pointDistance);
            }

            context.write(pointId, new Text(knnList.toString()));
        }
    }
}


