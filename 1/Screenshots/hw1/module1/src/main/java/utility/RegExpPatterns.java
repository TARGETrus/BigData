package utility;

/**
 * Utility class used to store regex patterns.
 */
public class RegExpPatterns {

    /**
     * An IP address.
     */
    public static String IP = "(ip\\d+)";

    /**
     * A single token (no spaces).
     */
    public static  String SINGLE_CHAR = "(\\S+)";

    /**
     * Something between [ and ].
     */
    public static  String SQUARE_BRACKETS = "\\[([^\\]]+)\\]";

    /**
     * A quoted string.
     */
    public static  String QUOTED = "\"([^\"]*?)\"";

    /**
     * Unsigned integer.
     */
    public static  String INTEGER = "(\\d+)";

    /**
     * Whitespaces.
     */
    public static  String WHITESPACES = "(\\s+)";
}
