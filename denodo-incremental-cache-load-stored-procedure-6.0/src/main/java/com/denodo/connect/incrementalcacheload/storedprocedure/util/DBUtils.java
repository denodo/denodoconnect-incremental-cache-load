package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Utility class for closing properly JDBC resources
 * 
 * @author acastro
 *
 */
public class DBUtils {

    public static void closeRs(final ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception ignored) {
        }
    }

    public static void closePs(final PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (Exception ignored) {
        }
    }

    public static void closeConn(final Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception ignored) {
        }
    }
}
