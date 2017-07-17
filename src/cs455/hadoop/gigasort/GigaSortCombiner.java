/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cs455.hadoop.gigasort;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class GigaSortCombiner extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable>{
    NullWritable nw = NullWritable.get();
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        
        for (NullWritable val : values) {
            
                context.write(key, nw);
            
        }

    }
}
