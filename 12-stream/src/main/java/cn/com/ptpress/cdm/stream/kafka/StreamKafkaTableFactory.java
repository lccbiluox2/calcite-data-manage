package cn.com.ptpress.cdm.stream.kafka;

import cn.com.ptpress.cdm.stream.StreamLogTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class StreamKafkaTableFactory  implements TableFactory<Table> {

    private static  Logger logger = LoggerFactory.getLogger(StreamKafkaTableFactory.class);

    @Override
    public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {

        Properties props = new Properties();
        props.put("bootstrap.servers", operand.getOrDefault("bootstrap.servers","localhost:9092"));
        props.put("topic", operand.getOrDefault("topic","topic_lcc"));
        props.put("group.id", operand.getOrDefault("group.id","group_lcc"));
        props.put("auto.offset.reset", operand.getOrDefault("auto.offset.reset","latest"));
        props.put("enable.auto.commit", operand.getOrDefault("enable.auto.commit","true"));
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        logger.info("kafka配置信息:{}",props);
        return new StreamKafkaLogTable(props);
    }
}
