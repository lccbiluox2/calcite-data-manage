package cn.com.ptpress.cdm.ds.pg;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * pg query
 *
 * @author jimo
 */
public class PostgreSqlQueryable<T> extends AbstractTableQueryable<T> {
    private PostgreSqlInfo info;
    private String tableName;

    public PostgreSqlQueryable(
            PostgreSqlInfo info,
            QueryProvider queryProvider, SchemaPlus schema, QueryableTable table, String tableName) {
        super(queryProvider, schema, table, tableName);
        this.info = info;
        this.tableName = tableName;
    }

    @Override
    public Queryable<T> where(FunctionExpression<? extends Predicate1<T>> predicate) {
        return super.where(predicate);
    }

    @Override
    public Enumerator<T> enumerator() {
        return (Enumerator<T>) query(null, null, -1, -1, null, null, null, null).enumerator();
    }

    public Enumerable<Object> query(List<Map.Entry<String, Class<?>>> fields,
                                    List<Map.Entry<String, String>> selectFields,
                                    Integer offset,
                                    Integer fetch,
                                    List<String> aggregate,
                                    List<String> group,
                                    List<String> predicates,
                                    List<String> order) {
        // 拼接SQL
        final StringBuilder sql = new StringBuilder();
        // todo: 这里要改一下，不然会一直报错
        //  PSQLException: ERROR: syntax error at or near "'movie'"
        //  ERROR: syntax error at or near "'movie'"
        //  Column 'code_name' not found in any table; did you mean 'CODE_NAME'?
        List<String> fieldNamesTemp = fields.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        List<String> fieldNames = new ArrayList<>();
        if(fieldNamesTemp != null && !fieldNamesTemp.isEmpty()){
            for (String item: fieldNamesTemp){
                fieldNames.add("\""+item+"\"");
            }
        }
        String fieldSql = fieldNames.stream().collect(Collectors.joining(","));
        final List<String> orderSql =
                order.stream().map(s -> s.split(" ")).map(item -> item[0] + " " + item[1]).collect(Collectors.toList());

        // 执行SQL=SELECT "CODE_NAME" FROM FILMS3 WHERE CODE_NAME = _ISO-8859-1'movie' LIMIT 2
        String whereItem = predicates.get(0);
        if(StringUtils.isNotBlank(whereItem)){
            // 这个where条件是一个整体 包含了一个 CODE_NAME = _ISO-8859-1'movie'
            if(whereItem.contains("_ISO-8859-1")){
                // 这里应该找个处理where的方法，但是我没找到相关的地方，这里只能先替换处理
                whereItem = whereItem.replace("_ISO-8859-1","");
            }
//            whereItem = "\""+whereItem+"\"";
        }

        sql.append("SELECT ").append(fieldSql).append(" FROM ").append(tableName);
        if (predicates.size() > 0) sql.append(" WHERE ").append(whereItem);
        if (group.size() > 0) sql.append(" GROUP BY ").append(String.join(",", group));
        if (order.size() > 0) sql.append(" ORDER BY ").append(String.join(",", orderSql));
        if (fetch >= 0) sql.append(" LIMIT ").append(fetch);

        System.out.println("最终执行SQL="+sql.toString());
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                try {
                    Class.forName("org.postgresql.Driver");

                    final Connection conn = DriverManager.getConnection(info.getUrl(), info.getUser(),
                            info.getPassword());
                    final Statement stmt = conn.createStatement();
                    final ResultSet rs = stmt.executeQuery(sql.toString());
                    return new PostgreSqlEnumerator(rs, fields);
                } catch (ClassNotFoundException | SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
