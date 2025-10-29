package org.apache.flink.connector.jdbc.dialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.flink.connector.jdbc.internal.converter.IgniteRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public class IgniteDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;

    public IgniteDialect() {
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:ignite:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new IgniteRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.ignite.IgniteJdbcThinDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields) {
        List<String> keys = Arrays.asList(uniqueKeyFields);
        String updateClause = (String) Arrays.stream(fieldNames).map((f) -> {
            return ":" + f;
        }).collect(Collectors.joining(", "));
        String sql = String.format(
                "MERGE INTO %s ( %s) VALUES(%s)",
                tableName,
                Joiner.on(",").join((Object[]) fieldNames),
                updateClause);
        return Optional.of(sql);
    }

    @Override
    public String dialectName() {
        return "Ignite";
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
