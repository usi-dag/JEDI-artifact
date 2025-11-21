package s2s.planner.calcite;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionUtil {

    public static CalciteConnection connection() {
        return connection(new Properties());
    }

    public static CalciteConnection connection(Properties info) {
        info.setProperty("caseSensitive", "false");
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            return connection.unwrap(CalciteConnection.class);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
