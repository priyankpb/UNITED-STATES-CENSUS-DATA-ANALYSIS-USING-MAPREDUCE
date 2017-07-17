package cs455.hadoop.gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class GigaSortMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    NullWritable nw = NullWritable.get();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            context.write(new LongWritable(Long.parseLong(itr.nextToken())), nw);
        }
    }
}
