package parsers;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Map;

public class UserAgentParserTest {

    /**
     * Parser class to be tested.
     */
    private UserAgentParser parser;

    // Tests - related variables.
    private static final String DEVICE = "Device";
    private static final String OS = "OS";
    private static final String BROWSER = "Browser";
    private static final String UA = "UA";

    @Before
    public void setUp() {
        parser = new UserAgentParser();
    }

    /**
     * Basic test checking user-agent string parsing works correctly.
     */
    @Test
    public void testEvaluateOk() {

        String userAgentString = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" +
                " (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36";

        Map<String, String> userAgentParsed = parser.evaluate(userAgentString);

        assertEquals(userAgentParsed.get(DEVICE), "Computer");
        assertEquals(userAgentParsed.get(OS), "Windows 10");
        assertEquals(userAgentParsed.get(BROWSER), "Chrome");
        assertEquals(userAgentParsed.get(UA), "Browser");
    }
}
