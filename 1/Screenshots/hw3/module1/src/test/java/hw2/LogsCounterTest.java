package hw2;

import entities.FloatIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import utility.UserAgentEnum;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * MRUnit-driven map-reduce test class.
 */
public class LogsCounterTest {

    private MapReduceDriver<LongWritable, Text, Text, FloatIntWritable, Text, FloatIntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        LogsCounterMapper mapper   = new LogsCounterMapper();
        LogsCounterReducer reducer = new LogsCounterReducer();

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    /**
     * Basic check of LogsCounter map-reduce output and counters.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testCorrectMapperAndReducerOutput()
            throws IOException {

        mapReduceDriver.withInput(new LongWritable(),
                new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\"" +
                        " 200 2155 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
                )
        );
        mapReduceDriver.withInput(new LongWritable(),
                new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\"" +
                        " 200 4256 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
                )
        );
        mapReduceDriver.withInput(new LongWritable(),
                new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\"" +
                        " 200 3389 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
                )
        );

        mapReduceDriver.withOutput(new Text("ip1"), new FloatIntWritable(3266.6667f, 9800));

        mapReduceDriver.runTest();

        Counters counters = mapReduceDriver.getCounters();
        assertEquals(3, counters.findCounter(UserAgentEnum.USER_AGENT.name(), "BOT").getValue());
    }
}
