package parsers;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.HashMap;
import java.util.Map;

@Description(
        name = "UserAgentParser",
        value = "Parses user agent string and extracts valuable data"
)
@UDFType(deterministic = false)
public class UserAgentParser extends UDF {

    public Map<String, String> evaluate(final String userAgentString) {

        Map<String, String> userAgentParsed = new HashMap<>();

        UserAgent userAgent = new UserAgent(userAgentString);

        userAgentParsed.put("Device", userAgent.getOperatingSystem().getDeviceType().getName());
        userAgentParsed.put("OS", userAgent.getOperatingSystem().getName());
        userAgentParsed.put("Browser", userAgent.getBrowser().getName());
        userAgentParsed.put("UA", userAgent.getBrowser().getBrowserType().getName());

        return userAgentParsed;
    }
}
