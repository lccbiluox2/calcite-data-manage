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

    /**
     * todo: 2024/3/17 10:33 九师兄
     * 测试点：测试使用流式处理连接kafka
     *
     * 日志> 消费条数:2
     * 日志> 消费到数据:{"LOG_TIME":"小红","LEVEL":"80","MSG":"lcc"}
     * 日志> 消费到数据:{"LOG_TIME":"小红","LEVEL":"80","MSG":"lcc"}
     * LOG_TIME	LEVEL	MSG
     * ------------------------------------------
     * 日志> 准备获取下一条数据..
     * 小红	80	lcc
     * 日志> 准备获取下一条数据..
     * 日志> 消费条数:2
     * 日志> 消费到数据:{"LOG_TIME":"小红","LEVEL":"80","MSG":"lcc"}
     * 日志> 消费到数据:{"LOG_TIME":"小红","LEVEL":"80","MSG":"lcc"}
     * 小红	80	lcc
     * 日志> 准备获取下一条数据..
     * 小红	80	lcc
     * 日志> 准备获取下一条数据..
     * 日志> 消费条数:2
     * 日志> 消费到数据:{"LOG_TIME":"小红","LEVEL":"80","MSG":"lcc"}
     * 日志> 消费到数据:{"LOG_TIME":"小红","LEVEL":"80","MSG":"lcc"}
     * 小红	80	lcc
     *
     * 可以看到正常打印了结果
     *
     * 【Calcite】Calcite简单流式处理-接入kafka案例
     * https://blog.csdn.net/qq_21383435/article/details/136776379
     **/
    @Test
    void testKafkaStreamWithCancel1() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_kafka.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM * from LOG");
            // 开启一个定时停止线程
            new Thread(() -> {
                try {
                    // 5秒后停止
                    TimeUnit.SECONDS.sleep(500);
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

    /**
     * todo: 2024/3/17 12:54 九师兄
     * 测试点：测试流式聚合
     *
     * java.sql.SQLException: Error while executing SQL "select STREAM level,count(*) from LOG group by level": From line 1, column 39 to line 1, column 52: Streaming aggregation requires at least one monotonic expression in GROUP BY clause
     *
     * 	at org.apache.calcite.avatica.Helper.createException(Helper.java:56)
     * 	at org.apache.calcite.avatica.Helper.createException(Helper.java:41)
     *
     **/
    @Test
    void testStreamGroupBy1() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_agg.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM level,count(*) from LOG group by level");

            printResult(rs);
        }
    }

    /**
     * todo: 2024/3/17 12:54 九师兄
     * 测试点：测试流式聚合 单调表达式
     *
     * 这条SOL 语句的含义就变成了：统计每分钟各个日志级别的日志数量，并且是不断统计，数据不断产牛。
     * 任是代码清单 12-14 所示的写法同样是有问题的，这个程序不 输出任何东西。这里其实 SQL 已经在运行了，
     * 数据全部都存储在内存当中，不会返回。
     **/
    @Test
    void testStreamGroupBy() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_agg.json");
        assert url != null;
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + url.getPath())) {
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select STREAM FLOOR(log_time TO SECOND) as " +
                    "log_time,level,count(*) as c from LOG group by FLOOR(log_time TO SECOND)," +
                    "level");
            printResult(rs);
        }
    }

    /**
     * todo: 2024/3/17 13:03 九师兄
     * 测试点：测试流统计 并且输出
     *
     * LOG_TIME	LEVEL	C
     * ------------------------------------------
     * 2024-03-17 05:05:00.0	DEBUG	2
     * 2024-03-17 05:05:00.0	WARN	3
     * 2024-03-17 05:05:00.0	INFO	7
     * 2024-03-17 05:05:00.0	ERROR	3
     *
     * LOG_TIME	LEVEL	C
     * ------------------------------------------
     * 2024-03-17 05:05:00.0	DEBUG	2
     * 2024-03-17 05:05:00.0	WARN	3
     * 2024-03-17 05:05:00.0	INFO	9
     * 2024-03-17 05:05:00.0	ERROR	3
     *
     * 【Calcite】Calcite 流式处理、流式聚合查询案例
     * https://blog.csdn.net/qq_21383435/article/details/136779997
     **/
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