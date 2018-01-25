package hw3;

import entities.TwoIntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Main driver class, provides configuration, setup & entry point.
 */
public class BidsAnalyzer {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        Path cacheFile = new Path(args[2]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Set output separator
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        // Create job
        Job job = Job.getInstance(conf, "BidsAnalyzer");
        job.setJarByClass(BidsAnalyzer.class);

        // Add designated file to distributed cache
        job.addCacheFile(cacheFile.toUri());

        // Setup MapReduce
        job.setMapperClass(BidsAnalyzerMapper.class);
        job.setCombinerClass(BidsAnalyzerReducer.class);
        job.setReducerClass(BidsAnalyzerReducer.class);
        job.setPartitionerClass(BidsPartitioner.class);
        job.setNumReduceTasks(3);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TwoIntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;

        System.exit(code);
    }
}
