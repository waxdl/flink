package org.apache.flink.connector.jdbc.dialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.OracleSQLRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public class OracleSQLDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;
    private static final String SQL_DEFAULT_PLACEHOLDER = " :";
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;

    public OracleSQLDialect() {
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new OracleSQLRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.driver.OracleDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "" + identifier + "";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields) {
        return Optional.of(this.getUpsertStatement(tableName, fieldNames, uniqueKeyFields, true));
    }

    public String getUpsertStatement(
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        StringBuilder mergeIntoSql = new StringBuilder();
        mergeIntoSql
                .append("MERGE INTO " + tableName + " T1 USING (")
                .append(this.buildDualQueryStatement(fieldNames))
                .append(") T2 ON (")
                .append(this.buildConnectionConditions(uniqueKeyFields) + ") ");
        String updateSql = this.buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);
        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql
                .append(" WHEN NOT MATCHED THEN ")
                .append("INSERT (")
                .append((String) Arrays.stream(fieldNames).map((col) -> {
                    return this.quoteIdentifier(col);
                }).collect(Collectors.joining(",")))
                .append(") VALUES (")
                .append((String) Arrays.stream(fieldNames).map((col) -> {
                    return "T2." + this.quoteIdentifier(col);
                }).collect(Collectors.joining(",")))
                .append(")");
        return mergeIntoSql.toString();
    }

    private String buildUpdateConnection(
            String[] fieldNames,
            String[] uniqueKeyFields,
            boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        String updateConnectionSql = (String) Arrays.stream(fieldNames).filter((col) -> {
            return !uniqueKeyList.contains(col.toLowerCase())
                    && !uniqueKeyList.contains(col.toUpperCase());
        }).map((col) -> {
            return this.buildConnectionByAllReplace(allReplace, col);
        }).collect(Collectors.joining(","));
        return updateConnectionSql;
    }

    private String buildConnectionByAllReplace(boolean allReplace, String col) {
        String conncetionSql = allReplace ?
                this.quoteIdentifier("T1") + "." + this.quoteIdentifier(col) + " = "
                        + this.quoteIdentifier("T2") + "." + this.quoteIdentifier(col) :
                this.quoteIdentifier("T1") + "." + this.quoteIdentifier(col) + " =nvl("
                        + this.quoteIdentifier("T2") + "." + this.quoteIdentifier(col) + ","
                        + this.quoteIdentifier("T1") + "." + this.quoteIdentifier(col) + ")";
        return conncetionSql;
    }

    private String buildConnectionConditions(String[] uniqueKeyFields) {
        return (String) Arrays.stream(uniqueKeyFields).map((col) -> {
            return "T1." + this.quoteIdentifier(col.trim()) + "=T2."
                    + this.quoteIdentifier(col.trim());
        }).collect(Collectors.joining(" and "));
    }

    public String buildDualQueryStatement(String[] column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect = (String) Arrays.stream(column).map((col) -> {
            return this.wrapperPlaceholder(col) + this.quoteIdentifier(col);
        }).collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM DUAL");
        return sb.toString();
    }

    public String wrapperPlaceholder(String fieldName) {
        return " :" + fieldName + " ";
    }

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public int maxDecimalPrecision() {
        return 65;
    }

    @Override
    public int minDecimalPrecision() {
        return 1;
    }

    @Override
    public int maxTimestampPrecision() {
        return 9;
    }

    @Override
    public int minTimestampPrecision() {
        return 1;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        return Arrays.asList(
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                LogicalTypeRoot.INTERVAL_DAY_TIME,
                LogicalTypeRoot.ARRAY,
                LogicalTypeRoot.MULTISET,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW,
                LogicalTypeRoot.DISTINCT_TYPE,
                LogicalTypeRoot.STRUCTURED_TYPE,
                LogicalTypeRoot.NULL,
                LogicalTypeRoot.RAW,
                LogicalTypeRoot.SYMBOL,
                LogicalTypeRoot.UNRESOLVED);
    }
}
