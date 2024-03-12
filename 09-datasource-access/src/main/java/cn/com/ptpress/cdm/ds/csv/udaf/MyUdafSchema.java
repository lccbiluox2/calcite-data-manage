package cn.com.ptpress.cdm.ds.csv.udaf;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class MyUdafSchema extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // 返回 MyUdafFun 的返回类型，这取决于你的实现
        // 这里假设返回类型是一个包含一个 VARCHAR 字段的记录
        System.out.println("返回类型...");
        return typeFactory.builder()
//                .add("result", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR))
                .add("result", typeFactory.createSqlType(SqlTypeName.ANY))
                .build();
    }
}
