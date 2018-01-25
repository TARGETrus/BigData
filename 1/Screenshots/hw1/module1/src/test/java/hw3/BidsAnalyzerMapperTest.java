package hw3;

import entities.TwoIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * MRUnit-driven mapper test class.
 */
public class BidsAnalyzerMapperTest {

    private MapDriver<LongWritable, Text, Text, TwoIntWritable> mapDriver;

    @Before
    public void setUp() {
        BidsAnalyzerMapper mapper = new BidsAnalyzerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    /**
     * Basic check of BidsAnalyzerMapper output.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testCorrectMapperOutput()
            throws IOException, URISyntaxException {

        mapDriver.withCacheFile(BidsAnalyzerMapperTest.class.getResource("/city.en.txt").toURI());

        mapDriver.withInput(new LongWritable(),
                new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\t" +
                        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t" +
                        "234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull" +
                        "\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t" +
                        "10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"
                )
        );

        mapDriver.withOutput(new Text("zhongshan"), new TwoIntWritable(1, 277));

        mapDriver.runTest();
    }

    /**
     * Check BidsAnalyzerMapper output in case of no city name was provided in file.
     * No output.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testNoMapperOutputWithoutCityName()
            throws IOException, URISyntaxException {

        mapDriver.withCacheFile(BidsAnalyzerMapperTest.class.getResource("/city.en.txt").toURI());

        mapDriver.withInput(new LongWritable(),
                new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\t" +
                        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t" +
                        "999\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull" +
                        "\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t" +
                        "10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"
                )
        );

        mapDriver.runTest();
    }

    /**
     * Check BidsAnalyzerMapper no output if line have wrong price.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testMapperNoOutputIfInvalidPrice()
            throws IOException, URISyntaxException {

        mapDriver.withCacheFile(BidsAnalyzerMapperTest.class.getResource("/city.en.txt").toURI());

        mapDriver.withInput(new LongWritable(),
                new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\t" +
                        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t" +
                        "234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull" +
                        "\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277$\t48\tnull\t2259\t" +
                        "10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"
                )
        );

        mapDriver.runTest();
    }

    /**
     * Check BidsAnalyzerMapper no output if line have small price.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testMapperNoOutputIfSmallPrice()
            throws IOException, URISyntaxException {

        mapDriver.withCacheFile(BidsAnalyzerMapperTest.class.getResource("/city.en.txt").toURI());

        mapDriver.withInput(new LongWritable(),
                new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\t" +
                        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t" +
                        "234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull" +
                        "\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t240\t48\tnull\t2259\t" +
                        "10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"
                )
        );

        mapDriver.runTest();
    }

    /**
     * Check BidsAnalyzerMapper no output if line have wrong chunks count.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testMapperNoOutputIfInvalidChunksCount()
            throws IOException, URISyntaxException {

        mapDriver.withCacheFile(BidsAnalyzerMapperTest.class.getResource("/city.en.txt").toURI());

        mapDriver.withInput(new LongWritable(),
                new Text("20131019025500549\t1\tCAD06D3WCtf\t" +
                        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t" +
                        "234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull" +
                        "\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t" +
                        "10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"
                )
        );

        mapDriver.runTest();
    }

    /**
     * Check BidsAnalyzerMapper no output if file was not found.
     *
     * @throws IOException in case runTest command fails.
     */
    @Test
    public void testNoMapperOutputWithoutCache()
            throws IOException {

        mapDriver.withInput(new LongWritable(),
                new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\t" +
                        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t" +
                        "234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull" +
                        "\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t" +
                        "10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"
                )
        );

        mapDriver.runTest();
    }
}
