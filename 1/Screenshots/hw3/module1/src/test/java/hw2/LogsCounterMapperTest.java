package hw2;

import entities.FloatIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * MRUnit-driven mapper test class.
 */
public class LogsCounterMapperTest {

    private MapDriver<LongWritable, Text, Text, FloatIntWritable> mapDriver;

    @Before
    public void setUp() {
        LogsCounterMapper mapper = new LogsCounterMapper();
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

        mapDriver.withInput(new LongWritable(),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\"" +
                                " 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
                        )
                );
        mapDriver.withInput(new LongWritable(),
                new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\"" +
                        " 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
                )
        );

        mapDriver.withOutput(new Text("ip1"), new FloatIntWritable(1f, 40028));
        mapDriver.withOutput(new Text("ip1"), new FloatIntWritable(1f, 56928));

        mapDriver.runTest();
    }
}
