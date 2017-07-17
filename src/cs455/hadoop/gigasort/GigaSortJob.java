package cs455.hadoop.gigasort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class GigaSortJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "Giga Sort");

            // Current class.
            job.setJarByClass(GigaSortJob.class);
            // Mapper
            job.setMapperClass(GigaSortMapper.class);
            job.setNumReduceTasks(32);
            job.setPartitionerClass(GigaSortPartitioner.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(GigaSortCombiner.class);
            // Reducer
            job.setReducerClass(GigaSortReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
