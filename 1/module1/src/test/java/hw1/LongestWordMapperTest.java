package hw1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * MRUnit-driven mapper test class.
 */
public class LongestWordMapperTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    /**
     * Basic check of LongestWordMapper output.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testCorrectMapperOutput()
            throws IOException {

        mapDriver.withInput(new LongWritable(), new Text(" KotOR universe KotOR smuggler, KotOR "));

        mapDriver.withOutput(new Text("smuggler,"), new IntWritable(9));

        mapDriver.runTest();
    }
}
