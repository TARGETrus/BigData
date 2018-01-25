package hw3;

import entities.TwoIntWritable;
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
public class BidsAnalyzerReducerTest {

    private ReduceDriver<Text, TwoIntWritable, Text, TwoIntWritable> reduceDriver;

    @Before
    public void setUp() {
        BidsAnalyzerReducer reducer = new BidsAnalyzerReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    /**
     * Basic check of BidsAnalyzerReducer output.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testCorrectReducerOutput()
            throws IOException {

        List<TwoIntWritable> bids = new ArrayList<>();

        bids.add(new TwoIntWritable(1, 240));
        bids.add(new TwoIntWritable(2, 250));
        bids.add(new TwoIntWritable(1, 260));
        bids.add(new TwoIntWritable(1, 280));

        reduceDriver.withInput(new Text("zhongshan"), bids);

        reduceDriver.withOutput(new Text("zhongshan"), new TwoIntWritable(5, 1030));

        reduceDriver.runTest();
    }
}
