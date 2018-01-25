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
 * MRUnit-driven combiner test class.
 */
public class LogsCounterCombinerTest {

    private ReduceDriver<Text, FloatIntWritable, Text, FloatIntWritable> reduceDriver;

    @Before
    public void setUp() {
        LogsCounterCombiner combiner = new LogsCounterCombiner();
        reduceDriver = ReduceDriver.newReduceDriver(combiner);
    }

    /**
     * Basic check of LongestWordCombiner output.
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

        reduceDriver.withOutput(new Text("ip1"), new FloatIntWritable(3f, 9800));

        reduceDriver.runTest();
    }
}
