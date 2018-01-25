package hw2;

import entities.FloatIntWritable;
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
public class LogsCounter {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Set output separator
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        // Enable output compression
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        // Set output compression codec to snappy
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        // Create job
        Job job = Job.getInstance(conf, "LogsCounter");
        job.setJarByClass(LogsCounter.class);

        // Setup MapReduce
        job.setMapperClass(LogsCounterMapper.class);
        job.setCombinerClass(LogsCounterCombiner.class);
        job.setReducerClass(LogsCounterReducer.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatIntWritable.class);

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

        // get all the user-agent job-related counters
        // for (Counter counter : job.getCounters().getGroup(UserAgentEnum.USER_AGENT.name())) {
        //     System.out.println(" - " + counter.getName() + ": "+counter.getValue());
        // }

        System.exit(code);
    }
}
