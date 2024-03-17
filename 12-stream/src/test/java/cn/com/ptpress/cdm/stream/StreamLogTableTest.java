package cn.com.ptpress.cdm.stream;

import jdk.nashorn.internal.ir.annotations.Ignore;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.*;
import java.util.concurrent.TimeUnit;

@Slf4j
class StreamLogTableTest {

    private void printResult(ResultSet rs) throws SQLException {
        final ResultSetMetaData md = rs.getMetaData();
        for (int i = 0; i < md.getColumnCount(); i++) {
            System.out.print(md.getColumnLabel(i + 1) + "\t");
        }
        System.out.println("\n------------------------------------------");
        while (rs.next()) {
            for (int i = 0; i < md.getColumnCount(); i++) {
                System.out.print(rs.getObject(i + 1) + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }

    /**
     * todo: 2024/3/17 09:23 九师兄
     * 测试点：测试简答的流式处理
     *
     * LOG_TIME	LEVEL	MSG
     * ------------------------------------------
     * 2024-03-17 01:22:59.927	INFO	This is a INFO msg on 1710638579927
     * 2024-03-17 01:23:00.809	DEBUG	This is a DEBUG msg on 1710638580809
     * 2024-03-17 01:23:01.16	DEBUG	This is a DEBUG msg on 1710638581160
     * 2024-03-17 01:23:01.969	ERROR	This is a ERROR msg on 1710638581969
     *
     * 【Calcite】Calcite简单流式处理案例
     *  https://blog.csdn.net/qq_21383435/article/details/136722828
     **/
    @Test
    void testStreamQuery() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_simple.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM * from LOG");
            // 永无止境的输出
            printResult(rs);
        }
    }

    /**
     * todo: 2024/3/17 09:30 九师兄
     * 测试点：测试流任务，主动5秒后停止查询
     *
     * LOG_TIME	LEVEL	MSG
     * ------------------------------------------
     * 2024-03-17 01:31:25.307	ERROR	This is a ERROR msg on 1710639085307
     * ...
     * 2024-03-17 01:31:29.373	WARN	This is a WARN msg on 1710639089373
     * 2024-03-17 01:31:29.983	INFO	This is a INFO msg on 1710639089983
     *
     * 可以看到5秒后自动停止了
     *
     * 【Calcite】Calcite简单流式处理案例
     *  https://blog.csdn.net/qq_21383435/article/details/136722828
     **/
    @Test
    void testStreamWithCancel() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_simple.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM * from LOG");
            // 开启一个定时停止线程
            new Thread(() -> {
                try {
                    // 5秒后停止
                    TimeUnit.SECONDS.sleep(5);
                    stmt.cancel();
                } catch (InterruptedException | SQLException e) {
                    e.printStackTrace();
                }
            }).start();
            // 永无止境的输出
            try {
                printResult(rs);
            } catch (SQLException e) {
                // ignore end
            }
        }
    }

    @Test
    void testStreamGroupBy1() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM level,count(*) from LOG group by level");

            printResult(rs);
        }
    }

    @Test
    void testStreamGroupBy() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
//            final Statement stmt = connection.createStatement();
//            final ResultSet rs = stmt.executeQuery("select STREAM level,count(*) from LOG group by level");

            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM FLOOR(log_time TO SECOND) as " +
                    "log_time,level,count(*) as c from LOG group by FLOOR(log_time TO SECOND)," +
                    "level");
            printResult(rs);
        }
    }

    @Test
    void testStreamGroupByCache() throws SQLException, InterruptedException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            // 模拟查询10次
            for (int i = 0; i < 10; i++) {
                TimeUnit.SECONDS.sleep(1);
                final Statement stmt = connection.createStatement();
                final ResultSet rs = stmt.executeQuery("select STREAM FLOOR(log_time TO MINUTE) as " +
                        "log_time,level,count(*) as c from LOG_CACHE group by FLOOR(log_time TO MINUTE)," +
                        "level");
                printResult(rs);
            }
        }
    }

    /**
     * TUMBLE和HOP还没有实现
     */
    @Ignore
    void testHop() throws InterruptedException, SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            // 模拟查询10次
            for (int i = 0; i < 10; i++) {
                TimeUnit.SECONDS.sleep(1);
                final Statement stmt = connection.createStatement();
                final ResultSet rs = stmt.executeQuery("select STREAM HOP_END(log_time, INTERVAL '1' HOUR, " +
                        "INTERVAL '3' HOUR) as log_time,count(*) as c from LOG_CACHE group by HOP(log_time, " +
                        "INTERVAL '1' HOUR, INTERVAL '3' HOUR)");
                printResult(rs);
            }
        }
    }
}