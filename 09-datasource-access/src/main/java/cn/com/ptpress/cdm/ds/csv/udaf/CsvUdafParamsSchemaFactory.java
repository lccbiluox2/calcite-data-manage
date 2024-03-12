package cn.com.ptpress.cdm.ds.csv.udaf;

import cn.com.ptpress.cdm.ds.csv.CsvSchema;
import cn.com.ptpress.cdm.ds.csv.udaf.MyUdafFun;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.util.Map;

public class CsvUdafParamsSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        //注册 udf
        System.out.println("准备注册udaf...");
//        parentSchema.add("MY_UDAF", new MyUdafSchema());
        parentSchema.add("COLLECT_LIST", AggregateFunctionImpl.create(MyUdafFun.class));
        return new CsvSchema(operand.get("dataFile").toString());
    }
}
