package cn.com.ptpress.cdm.ds.csv.udf;

import cn.com.ptpress.cdm.ds.csv.CsvSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class CsvUdfSQLFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        //注册 udf
        System.out.println("准备注册udf...");
        Object functions = operand.get("functions");
        System.out.println(functions.getClass());
        return new CsvSchema(operand.get("dataFile").toString());
    }
}
