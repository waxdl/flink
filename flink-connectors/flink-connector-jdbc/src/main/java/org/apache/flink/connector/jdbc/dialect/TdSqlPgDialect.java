package org.apache.flink.connector.jdbc.dialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.PostgresRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public class TdSqlPgDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;
    private static final int MAX_DECIMAL_PRECISION = 1000;
    private static final int MIN_DECIMAL_PRECISION = 1;

    public TdSqlPgDialect() {
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:tdsql-pg:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new PostgresRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.tencentcloud.tdsql.pg.jdbc.Driver");
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields) {
        String uniqueColumns = (String) Arrays
                .stream(uniqueKeyFields)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String updateClause = (String) Arrays.stream(fieldNames).map((f) -> {
            return this.quoteIdentifier(f) + "=EXCLUDED." + this.quoteIdentifier(f);
        }).collect(Collectors.joining(", "));
        return Optional.of(this.getInsertIntoStatement(tableName, fieldNames) + " ON CONFLICT ("
                + uniqueColumns + ") DO UPDATE SET " + updateClause);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String dialectName() {
        return "TdsqlPg";
    }

    @Override
    public int maxDecimalPrecision() {
        return 1000;
    }

    @Override
    public int minDecimalPrecision() {
        return 1;
    }

    @Override
    public int maxTimestampPrecision() {
        return 6;
    }

    @Override
    public int minTimestampPrecision() {
        return 1;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        return Arrays.asList(
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                LogicalTypeRoot.INTERVAL_DAY_TIME,
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
