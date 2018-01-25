package hw3;

import entities.TwoIntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utility.BidsAnalyzerCountersEnum;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Regular Mapper class.
 * Used for:
 * Read file,
 * split it by tabs,
 * find city ID, get city name by ID from Distributed cache,
 * send [city name | 1 | price] to combiner/reducer if price is more than provided in condition and city name was found.
 */
public class BidsAnalyzerMapper extends Mapper<LongWritable, Text, Text, TwoIntWritable> {

    private static final int ONE  = 1;

    private static final int CHUNKS_COUNT  = 24;
    private static final int CITY_ID_INDEX = 7;
    private static final int PRICE_INDEX   = 19;
    private static final int PRICE_MIN     = 250;

    private Map<String, String> cities = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        if (uris != null) {
            // Check Files loaded into Distributed Cache and find city.en.txt
            for (URI uri : uris) {
                if (uri.toString().contains("city.en.txt")) {
                    context.getCounter(BidsAnalyzerCountersEnum.FILE_FOUND).increment(1);
                    // Use BufferedReader to read file's contents and add them to cities map
                    try (BufferedReader cacheFileReader = new BufferedReader(
                            new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(uri))))) {
                        String lineRead;
                        while ((lineRead = cacheFileReader.readLine()) != null) {
                            String cityValues[] = lineRead.split("\\s+");
                            cities.put(cityValues[0].trim(), cityValues[1].trim());
                        }
                    } catch (FileNotFoundException e) {
                        // We don't want to ruin our Job because of file absence.
                        context.getCounter(BidsAnalyzerCountersEnum.FILE_READ_ERROR).increment(1);
                        e.printStackTrace();
                    }
                }
            }
        } else {
            context.getCounter(BidsAnalyzerCountersEnum.CACHE_EMPTY).increment(1);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");

        if (values.length != CHUNKS_COUNT)
            return;

        try {
            int price = Integer.valueOf(values[PRICE_INDEX]);
            String city = cities.get(values[CITY_ID_INDEX]);
            // Write in case of correct data only.
            if (price > PRICE_MIN && city != null) {
                context.write(new Text(city), new TwoIntWritable(ONE, price));
            }
        } catch (NumberFormatException e) {
            // Count invalid prices.
            context.getCounter(BidsAnalyzerCountersEnum.PRICE_INVALID).increment(1);
        }
    }
}
