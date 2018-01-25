package hw1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MRUnit-driven reducer test class.
 */
public class LongestWordReducerTest {

    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        LongestWordReducer reducer = new LongestWordReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    /**
     * Basic check of LongestWordReducer output.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testCorrectReducerOutput()
            throws IOException {

        List<IntWritable> wordLength1 = new ArrayList<>();
        List<IntWritable> wordLength2 = new ArrayList<>();
        List<IntWritable> wordLength3 = new ArrayList<>();

        wordLength1.add(new IntWritable(2));
        wordLength2.add(new IntWritable(6));
        wordLength3.add(new IntWritable(4));

        reduceDriver.withInput(new Text("ko"), wordLength1);
        reduceDriver.withInput(new Text("kokoko"), wordLength2);
        reduceDriver.withInput(new Text("koko"), wordLength3);

        reduceDriver.withOutput(new Text("kokoko"), new IntWritable(6));

        reduceDriver.runTest();
    }
}
