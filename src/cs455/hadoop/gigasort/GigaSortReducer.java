package cs455.hadoop.gigasort;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs. Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class GigaSortReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
    NullWritable nw = NullWritable.get();
    static int count = 0;
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        
        // calculate the total count
        for (NullWritable val : values) {
            count++;
            if (count % 1000 == 0 || count == 1) {
                context.write(key, nw);
            }
        }

    }
}
