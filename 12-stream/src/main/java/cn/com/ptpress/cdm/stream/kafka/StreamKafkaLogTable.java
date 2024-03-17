package cn.com.ptpress.cdm.stream.kafka;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StreamKafkaLogTable implements ScannableTable, StreamableTable {

    private Properties properties = new Properties();

    public StreamKafkaLogTable(Properties properties) {
        this.properties = properties;
    }

    /**
     * todo: 2024/3/17 10:52 九师兄
     * 这里不能改成 Enumerable<JSONObject> 因为这个接口定义死了
     * 必须是 Enumerable<Object[]> 数组类型
     **/
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new KafkaEnumerator<>(properties);
            }
        };
    }

    @Override
    public Table stream() {
        return this;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("LOG_TIME", SqlTypeName.VARCHAR)
                .add("LEVEL", SqlTypeName.VARCHAR)
                .add("MSG", SqlTypeName.VARCHAR)
                .build();
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(100d, new ArrayList<>(1), RelCollations.createSingleton(0));
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
                                                CalciteConnectionConfig config) {
        return false;
    }
}
