package cn.com.ptpress.cdm.ds.csv.udf;

import cn.com.ptpress.cdm.ds.csv.CsvSchema;
import cn.com.ptpress.cdm.ds.csv.udf.MyUdfFun;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.Map;

public class CsvUdfSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        //注册 udf
        System.out.println("准备注册udf...");
        parentSchema.add("EXAMPLE", ScalarFunctionImpl.create(MyUdfFun.class, "subString"));
        return new CsvSchema(operand.get("dataFile").toString());
    }
}
