package cn.com.ptpress.cdm.ds.pg;

import com.google.common.collect.ImmutableList;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.yandex.qatools.embed.postgresql.EmbeddedPostgres.cachedRuntimeConfig;
import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.V9_6;

/**
 * https://github.com/yandex-qatools/postgresql-embedded
 * http://www.h2database.com/html/main.html
 * https://github.com/opentable/otj-pg-embedded
 *
 * @author jimo
 */
public class EmbeddedPGDb {

    private static String url;
    private static String user = "postgres";
    private static String password = "postgres";
    private static EmbeddedPostgres postgres;

    @BeforeAll
    static void beforeAll() throws IOException {
        postgres = new EmbeddedPostgres(V9_6);
        /*
        https://github.com/yandex-qatools/postgresql-embedded
        第一次跑时，使用不带缓存的配置，会自动将postgresql-9.6.11-1-windows-x64-binaries下载到本地home目录
        然后拷贝并解压到任意目录，再使用缓存配置跑，避免每次解压
         */
        // url = postgres.start("localhost", 5432, "postgres", user, password, ImmutableList.of());
        String destPath = "/Users/lcc/soft/postgresql/calcite/pgsql";
        System.out.println("下载postgresql本地目录:" + destPath);
        IRuntimeConfig iRuntimeConfig = cachedRuntimeConfig(Paths.get(destPath));
        System.out.println("启动本地的postgresql...");
        url = postgres.start(iRuntimeConfig, "localhost", 5432, "postgres", user, password, ImmutableList.of());
        System.out.println("启动集成PG，url=" + url);
        // 从文件导入数据
        try {
            final File file = new File("src/main/resources/pg/data.sql");
            System.out.println("准备导入数据:" + file.getAbsolutePath());
            postgres.getProcess().get().importFromFile(file);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * todo: 2024/3/10 19:35 九师兄
     * <p>
     * 【PostgreSQL】PostgreSQL创建的表字段信息为什么小写了
     * https://blog.csdn.net/qq_21383435/article/details/135852873
     *
     * 【PostgreSQL】PostgreSQL命令行下如何修改某个模式下的字段长度
     *  https://blog.csdn.net/qq_21383435/article/details/134180994
     *
     * 准备执行SQL=select "CODE_NAME" from PG.films3  where "CODE_NAME" = 'movie' limit 2
     * 准备查询PG系统表信息...
     * 准备封装表信息table:films3
     * 准备获取表字段信息...
     * 获取表字段信息...column_name=CODE_NAME data_type=character varying
     * 准备获取pg RowType
     * 准备获取pg RowType
     * 准备执行 pg toRel
     * 创建PostgreSqlTableScan...
     * 调用register...
     * 出现bug的地方:"CODE_NAME" = 'movie'
     * 准备执行 pg PostgreSqlQueryable
     * 最终执行SQL=SELECT "CODE_NAME" FROM FILMS3 WHERE "CODE_NAME" = 'movie' LIMIT 2
     * movie
     *
     * 【Calcite】Calcite连接 PostgreSQL
     * https://blog.csdn.net/qq_21383435/article/details/136609230
     *
     **/
    @Test
    void testPG() throws SQLException {
        String sql = "select \"CODE_NAME\" from PG.films3  where \"CODE_NAME\" = 'movie' limit 2  ";

        System.out.println("准备执行SQL=" + sql);
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=src/main/resources/model_pg.json")) {
//            final Statement statement = conn.createStatement();
//            assertTrue(statement.execute("SELECT * FROM films limit 2"));
//            assertTrue(statement.getResultSet().next());
//            assertEquals("movie", statement.getResultSet().getString("code"));

            final Statement statement = conn.createStatement();
            final ResultSet rs = statement.executeQuery(sql);
            final int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    System.out.println(rs.getObject(i + 1));
                }
            }
        }
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }
}
