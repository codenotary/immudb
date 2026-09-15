import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * Utility class to help with immudb and PostgreSQL compatibility issues
 */
public class ImmudbTimestampHelper {

    /**
     * Converts a Java SQL Timestamp to a string format that immudb can accept
     *
     * @param timestamp The Java SQL Timestamp to convert
     * @return A string representation of the timestamp that immudb can accept
     */
    public static String formatTimestamp(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }

        // Format timestamp as ISO string that immudb can parse
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(timestamp);
    }
}
