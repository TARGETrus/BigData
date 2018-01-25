package hw1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Regular Mapper class.
 * Used for:
 * Read file,
 * split it by whitespaces,
 * find longest word,
 * send [longest word | length] to combiner/reducer.
 */
public class LongestWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String maxWord = "";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Regex is used because of input data can be poorly formatted.
        // Can be improved to cut off dots, etc.
        for(String word : line.split("\\s+")){
            if(!word.isEmpty() && word.length() > maxWord.length()){
                maxWord = word;
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        // Only the biggest word will be passed to combiner/reducer.
        context.write(new Text(maxWord), new IntWritable(maxWord.length()));
    }
}
