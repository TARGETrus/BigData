package hw2;

import entities.FloatIntWritable;
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
public class LogsCounterReducerTest {

    private ReduceDriver<Text, FloatIntWritable, Text, FloatIntWritable> reduceDriver;

    @Before
    public void setUp() {
        LogsCounterReducer reducer = new LogsCounterReducer();
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

        List<FloatIntWritable> wordLength1 = new ArrayList<>();

        wordLength1.add(new FloatIntWritable(1f, 2155));
        wordLength1.add(new FloatIntWritable(1f, 4256));
        wordLength1.add(new FloatIntWritable(1f, 3389));

        reduceDriver.withInput(new Text("ip1"), wordLength1);

        reduceDriver.withOutput(new Text("ip1"), new FloatIntWritable(3266.6667f, 9800));

        reduceDriver.runTest();
    }
}
