package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import oracle.jdbc.internal.OracleClob;
import oracle.sql.CHAR;

import java.io.BufferedReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DmOracleSQLRowConverter extends AbstractJdbcRowConverter {
    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "DmOracle";
    }

    public DmOracleSQLRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (val) -> {
                    return null;
                };
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return (val) -> {
                    return val;
                };
            case TINYINT:
                return (val) -> {
                    return ((Integer) val).byteValue();
                };
            case SMALLINT:
                return (val) -> {
                    return val instanceof Integer ? ((Integer) val).shortValue() : val;
                };
            case INTEGER:
                return (val) -> {
                    return val;
                };
            case BIGINT:
                return (val) -> {
                    return val;
                };
            case DECIMAL:
                int precision = ((DecimalType) type).getPrecision();
                int scale = ((DecimalType) type).getScale();
                return (val) -> {
                    return val instanceof BigInteger ? DecimalData.fromBigDecimal(new BigDecimal(
                            (BigInteger) val,
                            0), precision, scale) : DecimalData.fromBigDecimal(
                            (BigDecimal) val,
                            precision,
                            scale);
                };
            case DATE:
                return (val) -> {
                    return (int) ((Date) val).toLocalDate().toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (val) -> {
                    return (int) (((Time) val).toLocalTime().toNanoOfDay() / 1000000L);
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val) -> {
                    return val instanceof LocalDateTime ? TimestampData.fromLocalDateTime((LocalDateTime) val) : TimestampData
                            .fromTimestamp((Timestamp) val);
                };
            case CHAR:
            case VARCHAR:
                return (val) -> {
                    return val instanceof CHAR ? StringData.fromString(((CHAR) val).getString()) : (val instanceof Clob ? StringData
                            .fromString(((OracleClob) val).stringValue()) : StringData.fromString((String) val));
                };
            case BINARY:
            case VARBINARY:
                return (val) -> {
                    return (byte[]) ((byte[]) val);
                };
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static String clobToString(Clob clob) {
        String res = "";

        try {
            if (clob != null && clob.getCharacterStream() != null) {
                Reader io = clob.getCharacterStream();
                BufferedReader br = new BufferedReader(io);
                String s = br.readLine();

                StringBuffer sb;
                for (sb = new StringBuffer(); s != null; s = br.readLine()) {
                    sb.append(s);
                }

                res = new String(
                        sb.toString().getBytes(StandardCharsets.UTF_8),
                        StandardCharsets.UTF_8);
                if (res != null && res.getBytes(StandardCharsets.UTF_8).length > 4000) {
                    res = res
                            .trim()
                            .replaceAll("\t", " ")
                            .replaceAll("\r", " ")
                            .replaceAll("\n", " ")
                            .replaceAll("\\s+", " ");
                }
            }

            return res;
        } catch (Exception var6) {
            throw new UnsupportedOperationException("Clob type can't cast");
        }
    }
}
