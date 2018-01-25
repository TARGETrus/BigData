package hw2;

import entities.FloatIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Regular Reducer class.
 * Used for:
 * receive [ip [quantity | response size]] from mapper/combiner,
 * reduce to [ip [average size | total responses size]] for each ip,
 * write results into output.
 */
public class LogsCounterReducer extends Reducer<Text, FloatIntWritable, Text, FloatIntWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatIntWritable> value, Context context)
            throws IOException,InterruptedException {

        float quantity = 0;
        int   total    = 0;

        for (FloatIntWritable info : value) {
            quantity += info.getFloatValue();
            total += info.getIntValue();
        }

        float avg = (total / quantity);

        context.write(new Text(key.toString()), new FloatIntWritable(avg, total));
    }
}
