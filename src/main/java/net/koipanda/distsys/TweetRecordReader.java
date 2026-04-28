package net.koipanda.distsys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class TweetRecordReader extends RecordReader<LongWritable, Text> {
    private long start;
    private long end;
    private long pos;

    private final LongWritable key = new LongWritable();
    private final Text value = new Text();

    private FSDataInputStream fileIn;
    private LineReader lineReader;

    @Override
    public void initialize(InputSplit genSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genSplit;
        Configuration conf = context.getConfiguration();

        start = split.getStart();
        end = start + split.getLength();

        Path file = split.getPath();
        fileIn = file.getFileSystem(conf).open(file);
        fileIn.seek(start);
        lineReader = new LineReader(fileIn, conf);
        pos = start;

        if (start != 0) {
            Text dummy = new Text();
            while (pos < end) {
                int bytes = lineReader.readLine(dummy);
                if (bytes <= 0) break;

                pos += bytes;
                if (dummy.toString().trim().isEmpty()) break;
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (pos >= end) return false;

        Text line = new Text();
        StringBuilder tweet = new StringBuilder();
        long recordStart = pos;
        boolean sawContent = false;

        while (pos < end) {
            int bytes = lineReader.readLine(line);
            if (bytes <= 0) break;

            pos += bytes;
            String current = line.toString();

            if (current.trim().isEmpty()) {
                if (!tweet.isEmpty()) {
                    key.set(recordStart);
                    value.set(tweet.toString());
                    return true;
                }
                continue;
            }

            if (tweet.isEmpty()) recordStart = pos - bytes;
            tweet.append(current).append('\n');
            if (current.startsWith("W")) sawContent = true;

            if (sawContent) {
                int maybeBlank = lineReader.readLine(line);
                if (maybeBlank > 0) {
                    pos += maybeBlank;
                    String separator = line.toString();
                    if (!separator.trim().isEmpty()) {
                        tweet.append(separator).append('\n');
                    }
                }

                key.set(recordStart);
                value.set(tweet.toString());
                return true;
            }
        }

        if (!tweet.isEmpty()) {
            key.set(recordStart);
            value.set(tweet.toString());
            return true;
        }

        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        }
        return Math.min(1.0f, (pos - start) / (float) (end - start));
    }

    @Override
    public void close() throws IOException {
        if (lineReader != null) {
            lineReader.close();
        }
        if (fileIn != null) {
            fileIn.close();
        }
    }
}
