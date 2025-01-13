//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.jdbc.catalog;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

public class OracleTypeMapper {
    private final String databaseVersion;
    private final String driverVersion;

    public OracleTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }

    public static DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        int jdbcType = metadata.getColumnType(colIndex);
        metadata.getColumnName(colIndex);
        String oracleType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch(jdbcType) {
            case -104:
                return DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND());
            case -103:
                return DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH());
            case -102:
            case -101:
            case 93:
            case 2014:
                return scale > 0 ? DataTypes.TIMESTAMP(scale) : DataTypes.TIMESTAMP();
            case -15:
            case -9:
            case 1:
            case 12:
            case 2002:
            case 2005:
                return DataTypes.STRING();
            case -6:
            case 4:
            case 5:
                return DataTypes.INT();
            case 2:
            case 3:
                if (precision > 0 && precision < 38) {
                    return DataTypes.DECIMAL(precision, metadata.getScale(colIndex));
                }

                return DataTypes.DECIMAL(38, 18);
            case 6:
            case 7:
            case 100:
                return DataTypes.FLOAT();
            case 8:
            case 101:
                return DataTypes.DOUBLE();
            case 16:
                return DataTypes.BOOLEAN();
            case 91:
                return DataTypes.DATE();
            case 2004:
                return DataTypes.BYTES();
            default:
                String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(String.format("Doesn't support Oracle type '%s' on column '%s' .", oracleType, jdbcColumnName));
        }
    }
}
