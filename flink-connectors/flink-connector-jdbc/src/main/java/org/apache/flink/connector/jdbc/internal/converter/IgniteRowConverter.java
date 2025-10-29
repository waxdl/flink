package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

public class IgniteRowConverter extends AbstractJdbcRowConverter {
    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Ignite";
    }

    public IgniteRowConverter(RowType rowType) {
        super(rowType);
    }
}
