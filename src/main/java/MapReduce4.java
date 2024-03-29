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

    public static void run(String[] args) throws Exception {
        Job job = Job.getInstance(KnnMapReduce.knnConf, "knnMapReduce4");
        job.setJarByClass(MapReduce4.class);
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0] + "/output3"));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/output4"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }


    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        /**
         * Generate KeyValue Pair (PointId, knnList).
         * @param offset
         * @param knnInfoText
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable offset, Text knnInfoText, Context context)
                throws IOException, InterruptedException {
            String knnInfoString = knnInfoText.toString();
            PointInfo pointInfo = new PointInfo(knnInfoString, 1);

            context.write(new IntWritable(pointInfo.getPointId()), new Text(pointInfo.getKnnList().toString()));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        /**
         * Create a minimum Heap and add knn points from each potential cell to the heap.
         * Get knn points from the heap and add to knnList.
         * Generate KeyValue Pair (PointId, knnList)
         * @param pointId
         * @param knnListTextIterable
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(IntWritable pointId, Iterable<Text> knnListTextIterable, Context context)
                throws IOException, InterruptedException {
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            PriorityQueue<PointDistance> pointDistanceQueue = new PriorityQueue<>(new Util.PointDistanceComparator());
            for (Text knnListText : knnListTextIterable) {
                String knnListString = knnListText.toString();
                ArrayList<PointDistance> pointDistanceList = Util.knnListStringToKnnList(knnListString);
                pointDistanceQueue.addAll(pointDistanceList);
            }

            ArrayList<PointDistance> knnList = Util.createKnnListFromQueue(pointDistanceQueue, k);
            
            context.write(pointId, new Text(knnList.toString()));
        }
    }
}


