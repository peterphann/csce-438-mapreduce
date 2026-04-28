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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TweetSleepCount {

    public static class SleepMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final IntWritable outHour = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tweet = value.toString();
            String[] lines = tweet.split("\\r?\\n");

            if (lines.length < 3) return;

            String timeLine = lines[0].trim();
            String contentLine = lines[2].trim();

            if (!timeLine.startsWith("T")) return;
            if (!contentLine.startsWith("W")) return;

            String content = contentLine.substring(1).trim().toLowerCase(Locale.ROOT);
            if (!content.contains("sleep")) return;

            String[] parts = timeLine.split("\\s+");
            if (parts.length < 3) return;

            String[] timePieces = parts[2].split(":");
            if (timePieces.length < 1) return;

            try {
                int hour = Integer.parseInt(timePieces[0]);
                outHour.set(hour);
                context.write(outHour, ONE);
            } catch (NumberFormatException ignored) {}

        }
    }

    public static class SleepReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final Map<Integer, Integer> counts = new HashMap<>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
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
            System.err.println("Usage: TweetSleepCount <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tweet sleep count");
        job.setJarByClass(TweetSleepCount.class);

        job.setInputFormatClass(TweetInputFormat.class);
        job.setMapperClass(SleepMapper.class);
        job.setReducerClass(SleepReducer.class);

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
