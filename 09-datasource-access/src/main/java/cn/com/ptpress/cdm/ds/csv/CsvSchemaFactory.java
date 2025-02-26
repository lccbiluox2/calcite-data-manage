package cn.com.ptpress.cdm.ds.csv;

import cn.com.ptpress.cdm.ds.csv.udaf.MyUdafFun;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.util.Map;

public class CsvSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new CsvSchema(operand.get("dataFile").toString());
    }
}
