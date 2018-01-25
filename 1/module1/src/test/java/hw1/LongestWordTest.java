package hw1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * MRUnit-driven map-reduce test class.
 */
public class LongestWordTest {

    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        LongestWordReducer reducer = new LongestWordReducer();

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    /**
     * Basic check of LongestWord map-reduce output.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testCorrectMapperAndReducerOutput()
            throws IOException {

        mapReduceDriver.withInput(new LongWritable(), new Text(" KotOR universe KotOR smuggler, KotOR "));
        mapReduceDriver.withOutput(new Text("smuggler,"), new IntWritable(9));

        mapReduceDriver.runTest();
    }
}
