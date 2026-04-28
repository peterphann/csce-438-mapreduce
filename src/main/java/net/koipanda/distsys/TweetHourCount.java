package net.koipanda.distsys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TweetHourCount {

    public static class HourMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final IntWritable outHour = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (!line.startsWith("T ")) return;

            String[] parts = line.split("\\s+");
            if (parts.length < 3) return;

            String timePart = parts[2];
            String[] timePieces = timePart.split(":");
            if (timePieces.length < 1) return;

            try {
                int hour = Integer.parseInt(timePieces[0]);
                outHour.set(hour);
                context.write(outHour, ONE);
            } catch (NumberFormatException ignored) {}
        }

    }

    public static class HourReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final Map<Integer, Integer> counts = new HashMap<>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            counts.put(key.get(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int hour = 0; hour < 24; hour++) {
                int count = counts.getOrDefault(hour, 0);
                context.write(new IntWritable(hour), new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: TweetHourCount <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tweet hour count");
        job.setJarByClass(TweetHourCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HourMapper.class);
        job.setReducerClass(HourReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
