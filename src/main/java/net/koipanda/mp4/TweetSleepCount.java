package net.koipanda.mp4;

import net.koipanda.mp4.util.TweetTimeUtil;
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
import java.util.Locale;
import java.util.OptionalInt;

public class TweetSleepCount {

    public static class SleepMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text hourBucketKey = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tweet = value.toString();
            String[] lines = tweet.split("\\r?\\n");

            if (lines.length < 3) return;

            String timeLine = lines[0].trim();
            String contentLine = lines[2].trim();

            if (!timeLine.startsWith("T")) return;
            if (!contentLine.startsWith("W")) return;

            // check if it contains sleep -- this matches any iteration of sleep (sleepy, asleep, etc) :D
            String content = contentLine.substring(1).trim().toLowerCase(Locale.ROOT);
            if (!content.contains("sleep")) return;

            OptionalInt hourOpt = TweetTimeUtil.extractHourFromTimeLine(timeLine);
            if (hourOpt.isEmpty()) return;

            hourBucketKey.set(TweetTimeUtil.toHourBucketLabel(hourOpt.getAsInt()));
            context.write(hourBucketKey, ONE);
        }
    }

    public static class SleepReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
