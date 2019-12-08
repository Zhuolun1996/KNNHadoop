import java.io.IOException;

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

public class MapReduce1 {

    private static final Logger LOG = Logger.getLogger(MapReduce1.class);
    private static Configuration knnConf;
    private static Integer size;
    private static Integer grain;

    public static void run(String[] args, Configuration knnConf) throws Exception {
        MapReduce1.knnConf = knnConf;
        size = Integer.parseInt(knnConf.get("size"));
        grain = Integer.parseInt(knnConf.get("grain"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "knnMapReduce1");
        job.setJarByClass(MapReduce1.class);
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0] + "/input"));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/output1"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }


    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public IntWritable getCellIdByCoordinate(String coordinate) {
            Integer x = Integer.parseInt(coordinate.split(",")[1]);
            Integer y = Integer.parseInt(coordinate.split(",")[2]);

            Integer cellWidth = size / grain;
            Integer row = y / cellWidth;
            Integer column = x / cellWidth;

            return new IntWritable(row * grain + column);

        }

        @Override
        public void map(LongWritable offset, Text coordinateText, Context context)
                throws IOException, InterruptedException {
            String coordinateString = coordinateText.toString();
            context.write(getCellIdByCoordinate(coordinateString), one);
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable cellId, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(cellId, new IntWritable(sum));
        }
    }
}


