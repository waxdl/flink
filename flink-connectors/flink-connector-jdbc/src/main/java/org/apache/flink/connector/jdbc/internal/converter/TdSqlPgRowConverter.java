package org.apache.flink.connector.jdbc.internal.converter;

import java.lang.reflect.Array;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

public class TdSqlPgRowConverter extends AbstractJdbcRowConverter {
    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "TdsqlPg";
    }

    public TdSqlPgRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return this.createPostgresArrayConverter(arrayType);
        } else {
            return this.createPrimitiveConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        return root == LogicalTypeRoot.ARRAY ? (val, index, statement) -> {
            throw new IllegalStateException(String.format(
                    "Writing ARRAY type is not yet supported in JDBC:%s.",
                    this.converterName()));
        } : super.createNullableExternalConverter(type);
    }

    private JdbcDeserializationConverter createPostgresArrayConverter(ArrayType arrayType) {
        Class elementClass;
        JdbcDeserializationConverter elementConverter;
        if (LogicalTypeChecks.hasFamily(
                arrayType.getElementType(),
                LogicalTypeFamily.BINARY_STRING)) {
            elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
            elementConverter = this.createNullableInternalConverter(arrayType.getElementType());
            return (val) -> {
                PgArray pgArray = (PgArray) val;
                Object[] in = (Object[]) ((Object[]) pgArray.getArray());
                Object[] array = (Object[]) ((Object[]) Array.newInstance(elementClass, in.length));

                for (int i = 0; i < in.length; ++i) {
                    array[i] = elementConverter.deserialize(((PGobject) in[i])
                            .getValue()
                            .getBytes());
                }

                return new GenericArrayData(array);
            };
        } else {
            elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
            elementConverter = this.createNullableInternalConverter(arrayType.getElementType());
            return (val) -> {
                PgArray pgArray = (PgArray) val;
                Object[] in = (Object[]) ((Object[]) pgArray.getArray());
                Object[] array = (Object[]) ((Object[]) Array.newInstance(elementClass, in.length));

                for (int i = 0; i < in.length; ++i) {
                    array[i] = elementConverter.deserialize(in[i]);
                }

                return new GenericArrayData(array);
            };
        }
    }

    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        return super.createInternalConverter(type);
    }
}
