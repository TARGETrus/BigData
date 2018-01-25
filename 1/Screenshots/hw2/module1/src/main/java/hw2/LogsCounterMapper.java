package hw2;

import entities.FloatIntWritable;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utility.RegExpPatterns;
import utility.UserAgentEnum;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Regular Mapper class.
 * Used for:
 * parse logs and send [ip [quantity | response size]] to combiner/reducer.
 */
public class LogsCounterMapper extends Mapper<LongWritable, Text, Text, FloatIntWritable> {

    private static final int SIZE_INDEX    = 7;
    private static final int BROWSER_INDEX = 9;

    // Constructing resulting regex from chunks to parse logs.
    private String rex = String.join(" ",
                                    RegExpPatterns.IP,
                                    RegExpPatterns.SINGLE_CHAR, RegExpPatterns.SINGLE_CHAR,
                                    RegExpPatterns.SQUARE_BRACKETS, RegExpPatterns.QUOTED,
                                    RegExpPatterns.INTEGER, RegExpPatterns.INTEGER,
                                    RegExpPatterns.QUOTED, RegExpPatterns.QUOTED);

    private Pattern pat = Pattern.compile(rex);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Matcher mat = pat.matcher(value.toString());

        if (mat.matches()) {

            Browser browser = new UserAgent(mat.group(BROWSER_INDEX)).getBrowser();
            context.getCounter(UserAgentEnum.USER_AGENT.name(), browser.name()).increment(1);

            context.write(
                    new Text(mat.group(1)),
                    new FloatIntWritable(1f, Integer.valueOf(mat.group(SIZE_INDEX)))
            );
        }
    }
}
