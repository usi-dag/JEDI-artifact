package s2s.planner.calcite;

import s2s.planner.qp.Field;
import s2s.planner.qp.Schema;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class SchemaTable extends AbstractTable implements TranslatableTable {
    private final Schema schema;

    private RelDataType dataType = null;

    public SchemaTable(Schema schema) {
        this.schema = schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if(dataType == null) {
            List<RelDataType> fieldTypesConverted = new LinkedList<>();
            List<String> fieldNames = new LinkedList<>();
            for (Field field : schema.getFields()) {
                SqlTypeName sqlTypeName = switch (field.getType().getSimpleName()) {
                    case "int", "Integer" -> SqlTypeName.INTEGER;
                    case "double", "Double" -> SqlTypeName.DOUBLE;
                    case "long", "Long" -> SqlTypeName.BIGINT;
                    case "Date" -> SqlTypeName.DATE;
                    case "String" -> SqlTypeName.VARCHAR;

                    default ->
                            throw new IllegalStateException("Unexpected field type: " + field.getType().getSimpleName());
                };
                RelDataType colType = new BasicSqlType(typeFactory.getTypeSystem(), sqlTypeName);
                if(field.getType() == String.class) {
                    // TODO how to get rid?
                    colType = typeFactory.createTypeWithCharsetAndCollation(colType, StandardCharsets.ISO_8859_1, SqlCollation.IMPLICIT);
                }
                if(field.isNullable()) {
                    colType = typeFactory.createTypeWithNullability(colType, true);
                }
                fieldTypesConverted.add(colType);
                fieldNames.add(field.getName());
            }
            dataType = typeFactory.createStructType(fieldTypesConverted, fieldNames);
        }
        return dataType;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return EnumerableTableScan.create(context.getCluster(), relOptTable);
    }
}
