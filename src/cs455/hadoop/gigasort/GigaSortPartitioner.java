/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cs455.hadoop.gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author priyankb
 */
public class GigaSortPartitioner extends Partitioner<LongWritable, NullWritable> {

    @Override
    public int getPartition(LongWritable key, NullWritable value, int n) {
        long divider = (long) ((((double) Long.MAX_VALUE) + 1) / n);
        long keyLong = key.get();
        int partNum = (int) (keyLong / divider);

        return partNum;
    }
}
