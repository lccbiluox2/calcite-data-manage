package cn.com.ptpress.cdm.ds.csv;

import java.sql.*;

public class CsvUdfAndUdafApi {


    /**
     * 测试 sql 并打印结果
     *
     * @param st  连接
     * @param sql 输入的sql
     * @throws SQLException
     */
    static void testSql(Statement st, String sql) throws SQLException {
        ResultSet result = st.executeQuery(sql);
        int columnCount = result.getMetaData().getColumnCount();
        //打印sql
        System.out.println(sql);
        while (result.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(result.getString(i)+"     ");
            }
            System.out.println();
        }
        result.close();
    }
}
