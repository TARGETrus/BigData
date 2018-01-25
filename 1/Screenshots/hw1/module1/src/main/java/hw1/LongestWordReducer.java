package hw1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Regular Reducer class. Also can be used as combiner.
 * Used for:
 * receive [longest word | length] from mapper/combiner,
 * find longest word,
 * write results into output.
 */
public class LongestWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String maxWord = "";

    @Override
    protected void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws java.io.IOException,InterruptedException {
        // Replace maxWord if found longer.
        if (key.toString().length() > maxWord.length()) {
            maxWord = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        // Single entry per reducer output file ensured by calling this write method on cleanup.
        context.write(new Text(maxWord), new IntWritable(maxWord.length()));
    }
}
