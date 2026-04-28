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

    private LongWritable key = new LongWritable();
    private Text value = new Text();

    private FSDataInputStream fileIn;
    private LineReader lineReader;

    @Override
    public void initialize(InputSplit genSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genSplit;
        Configuration conf = context.getConfiguration();

        start = split.getStart();
        end = split.getLength();

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



}
