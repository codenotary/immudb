import java.sql.Timestamp;
import java.sql.Statement;

// Method 1: Using a formatted string instead of Timestamp object
private void insertUserWithFormattedTimestamp() throws SQLException {
    final String INSERT_QUERY_USERS = "INSERT INTO USERS (username,created_at) VALUES (?,?)";
    String urlForConnection = "jdbc:postgresql://"+url+":"+port+"/"+databaseName+"?sslmode=allow&preferQueryMode=simple";
    var connection = DriverManager.getConnection(urlForConnection,"immudb", "immudb");
    var client = connection.getClient();
    PreparedStatement pstmt = client.prepareStatement(INSERT_QUERY_USERS);

    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    pstmt.setString(1, "TestUserName");
    // Convert the timestamp to a string format immudb can handle
    pstmt.setString(2, ImmudbTimestampHelper.formatTimestamp(timestamp));
    pstmt.execute();
}

// Method 2: Using SQL CAST with a formatted timestamp
private void insertUserWithTimestampCast() throws SQLException {
    final String INSERT_QUERY_USERS =
        "INSERT INTO USERS (username,created_at) VALUES (?, CAST(? AS TIMESTAMP))";
    String urlForConnection = "jdbc:postgresql://"+url+":"+port+"/"+databaseName+"?sslmode=allow&preferQueryMode=simple";
    var connection = DriverManager.getConnection(urlForConnection,"immudb", "immudb");
    var client = connection.getClient();
    PreparedStatement pstmt = client.prepareStatement(INSERT_QUERY_USERS);

    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    pstmt.setString(1, "TestUserName");
    pstmt.setString(2, ImmudbTimestampHelper.formatTimestamp(timestamp));
    pstmt.execute();
}

// Method 3: Using direct string concatenation (less secure, avoid with user input)
private void insertUserWithDirectQuery() throws SQLException {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String formattedTimestamp = ImmudbTimestampHelper.formatTimestamp(timestamp);

    String query = String.format(
        "INSERT INTO USERS (username,created_at) VALUES ('TestUserName', '%s')",
        formattedTimestamp
    );

    String urlForConnection = "jdbc:postgresql://"+url+":"+port+"/"+databaseName+"?sslmode=allow&preferQueryMode=simple";
    var connection = DriverManager.getConnection(urlForConnection,"immudb", "immudb");
    Statement stmt = connection.createStatement();
    stmt.execute(query);
}
