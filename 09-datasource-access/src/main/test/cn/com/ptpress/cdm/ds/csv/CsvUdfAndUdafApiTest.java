package cn.com.ptpress.cdm.ds.csv;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

class CsvUdfAndUdafApiTest {

    private CsvUdfAndUdafApi csvTest = new CsvUdfAndUdafApi();

    /**
     * todo: 2024/3/11 21:47 九师兄
     * 测试点： 简单测试读取csv文件
     *
     * select * from CSV."data_01"
     * 1     小明     90
     * 2     小红     98
     * 3     小亮     95
     *
     * 【Calcite】Calcite注册自定义UDF案例
     *  https://blog.csdn.net/qq_21383435/article/details/136637174
     **/
    @Test
    void main1() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();
        csvTest.testSql(st, "select * from CSV.\"data_01\"");
        connection.close();
    }


    /**
     * todo: 2024/3/11 21:51 九师兄
     * 测试点：测试直接使用calcite系统函数
     *
     * select ABS("Score") from CSV."data_01"
     * 90
     * 98
     * 95
     *
     * 【Calcite】Calcite注册自定义UDF案例
     *  https://blog.csdn.net/qq_21383435/article/details/136637174
     **/
    @Test
    void main2() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();
        csvTest.testSql(st, "select ABS(\"Score\") from CSV.\"data_01\" ");
        connection.close();
    }

    /**
     * todo: 2024/3/11 21:56 九师兄
     * 测试点: 测试使用schema注册函数
     *
     * 准备注册udf...
     * select EXAMPLE("Name") from CSV."data_01"
     * 小明
     * 小红
     * 小亮
     *
     *
     * 【Calcite】Calcite注册自定义UDF案例
     *  https://blog.csdn.net/qq_21383435/article/details/136637174
     **/
    @Test
    void main3() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv_udf_schema.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();

        csvTest.testSql(st, "select EXAMPLE(\"Name\") from CSV.\"data_01\" ");
        connection.close();
    }


    /**
     * todo: 2024/3/11 21:56 九师兄
     * 测试点: 测试使用 model_csv_udf_funs.json 注册函数
     *
     * Exception: Error instantiating JsonCustomSchema(name=CSV)
     *
     * 暂时还不知道怎么写
     *
     *
     * 【Calcite】Calcite注册自定义UDF案例
     *  https://blog.csdn.net/qq_21383435/article/details/136637174
     **/
    @Test
    void main31() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv_udf_funs.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();

        csvTest.testSql(st, "select EXAMPLE(\"Name\") from CSV.\"data_01\" ");
        connection.close();
    }

    /**
     * todo: 2024/3/11 21:56 九师兄
     * 测试点: 测试使用 SQL 注册函数
     *
     * Exception: Error instantiating JsonCustomSchema(name=CSV)
     *
     * 暂时还不知道怎么写
     *
     *
     * 【Calcite】Calcite注册自定义UDF案例
     *  https://blog.csdn.net/qq_21383435/article/details/136637174
     **/
    @Test
    void main32() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv_udf_sql.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();
        st.execute("create function \"EXAMPLE\"\n as 'cn.com.ptpress.cdm.ds.csv.udf.MyUdfFun'");

        csvTest.testSql(st, "select EXAMPLE(\"Name\") from CSV.\"data_01\" ");
        connection.close();
    }



    /**
     * todo: 2024/3/11 22:00 九师兄
     * 测试点：测试注册udf 多个参数该怎么写
     *
     * 准备注册udf...
     * select EXAMPLE("Name",1) from CSV."data_01"
     * 小
     * 小
     * 小
     *
     *
     * 【Calcite】Calcite注册自定义UDF案例
     *  https://blog.csdn.net/qq_21383435/article/details/136637174
     **/
    @Test
    void main4() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv_udf_params.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();

        csvTest.testSql(st, "select EXAMPLE(\"Name\",1) from CSV.\"data_01\"");
        connection.close();
    }

    @Test
    void main5() throws SQLException {
        URL url = ClassLoader.getSystemClassLoader().getResource("model_csv_udaf.json");
        String path = url.getPath();
        System.out.println("文件路径:" + path);
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + path);
        Statement st = connection.createStatement();
        csvTest.testSql(st, "select COLLECT_LIST(\"Name\") from \"CSV.data_01\"");
        connection.close();
    }


}