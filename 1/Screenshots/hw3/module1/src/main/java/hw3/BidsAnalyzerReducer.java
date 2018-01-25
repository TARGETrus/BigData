package hw3;

import entities.TwoIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Regular Reducer class.
 * Used for:
 * receive [city name | bids quantity | total bids price] from mapper/combiner,
 * reduce values,
 * write results into output.
 */
public class BidsAnalyzerReducer extends Reducer<Text, TwoIntWritable, Text, TwoIntWritable> {

    @Override
    protected void reduce(Text key, Iterable<TwoIntWritable> value, Context context)
            throws IOException,InterruptedException {

        int totalBids = 0;
        int sum       = 0;

        for (TwoIntWritable info : value) {
            totalBids += info.getIntValueOne();
            sum       += info.getIntValueTwo();
        }

        context.write(new Text(key.toString()), new TwoIntWritable(totalBids, sum));
    }
}
