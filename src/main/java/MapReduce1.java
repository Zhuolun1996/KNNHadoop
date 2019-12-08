import java.io.IOException;

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

    public static void run(String[] args) throws Exception {
        Job job = Job.getInstance(KnnMapReduce.knnConf, "knnMapReduce1");
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

        /**
         * Get cell id by the data point's coordination.
         * @param coordinate
         * @param size
         * @param grain
         * @return
         */
        public IntWritable getCellIdByCoordinate(String coordinate, int size, int grain) {
            int x = Integer.parseInt(coordinate.split(",")[1]);
            int y = Integer.parseInt(coordinate.split(",")[2]);

            int cellWidth = size / grain;
            int row = y / cellWidth;
            int column = x / cellWidth;

            return new IntWritable(row * grain + column);

        }

        /**
         * Map KeyValue Pair (CellId, 1).
         * @param offset
         * @param coordinateText
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable offset, Text coordinateText, Context context)
                throws IOException, InterruptedException {
            String coordinateString = coordinateText.toString();
            int size = Integer.parseInt(context.getConfiguration().get("size"));
            int grain = Integer.parseInt(context.getConfiguration().get("grain"));
            context.write(getCellIdByCoordinate(coordinateString, size, grain), one);
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        /**
         * Reduce KeyValue Pair (CellId, 1), count number of points in each cell.
         * Generate KeyValue Pair (CellId, number of points).
         * @param cellId
         * @param counts
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
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


