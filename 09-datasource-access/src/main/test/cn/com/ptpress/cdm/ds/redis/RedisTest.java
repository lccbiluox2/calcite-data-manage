package cn.com.ptpress.cdm.ds.redis;


import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.jupiter.api.*;

import java.net.URL;
import java.sql.*;

class RedisTest {
    private RedisServerStart redisServer;

    private void query(String sql, int expected) throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_redis.json");
        String rul = "jdbc:calcite:model=" + url.getPath();
        System.out.println("连接信息:" + rul);
        Connection connection =
                DriverManager.getConnection(rul);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        Statement st = calciteConnection.createStatement();
        ResultSet resultSet = st.executeQuery(sql);
        showQueryResult(resultSet);
    }

    private void showQueryResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        System.out.println("列："+columnCount);
        while(resultSet.next()){
            String id = resultSet.getString(1);
            String name = resultSet.getString(2);
            System.out.println("id:"+id+" value："+name);
        }
    }


    @BeforeEach
    void startUp() throws Exception {
        redisServer = RedisServerStart.getInstance();
        System.out.println("准备启动redis..");
        redisServer.startServer();
    }

    @AfterEach
    void stop() {
        redisServer.stopServer();
    }

    /**
     * todo: 2024/3/10 17:45 九师兄
     * 测试点:测试calcite连接redis
     * 准备启动redis..
     * redis 已经启动完成
     * 添加数据..
     * 连接信息:jdbc:calcite:model=/Users/lcc/IdeaProjects/github/calcite-data-manage/09-datasource-access/target/classes/model_redis.json
     * 准备创建schema:host=localhost port=6379 database=0 tables=1
     * mapItem.name->stu_01
     * 准备创建redis表:stu_01
     * redis config RedisConfig[host='localhost', port=6379, database=0, password='']
     * tableMap->null
     * tableMap->{stu_01=cn.com.ptpress.cdm.ds.redis.RedisTable@70242f38}
     * 列：2
     * id:"小红" value：80
     * id:"小明" value：90
     * redis 已停止
     **/
    @Test
    public void testServer() throws SQLException, ClassNotFoundException {
        query("select * from \"redis_shcema\".\"stu_01\" ", 2);
    }


}
