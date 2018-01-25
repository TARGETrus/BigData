package hw3;

import entities.TwoIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BidsPartitioner extends Partitioner<Text, TwoIntWritable> {

    /**
     * Number of partition based on bids quantity and price.
     *
     * @return number of partition
     */
    @Override
    public int getPartition(Text key, TwoIntWritable info, int numPartitions) {
        return Math.abs(info.getIntValueTwo() % numPartitions);
    }
}
