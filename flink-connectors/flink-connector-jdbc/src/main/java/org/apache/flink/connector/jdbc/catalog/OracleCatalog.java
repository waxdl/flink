//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.jdbc.catalog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import oracle.jdbc.OracleDriver;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleCatalog extends AbstractJdbcCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(OracleCatalog.class);
    public static final String DEFAULT_DATABASE = "oracle-catalog";
    private static final Set<String> builtinDatabases = new HashSet<String>() {
        {
            this.add("SCOTT");
            this.add("ANONYMOUS");
            this.add("XS$NULL");
            this.add("DIP");
            this.add("SPATIAL_WFS_ADMIN_USR");
            this.add("SPATIAL_CSW_ADMIN_USR");
            this.add("APEX_PUBLIC_USER");
            this.add("ORACLE_OCM");
            this.add("MDDATA");
        }
    };

    public OracleCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        String driverVersion = (String)Preconditions.checkNotNull(OracleDriver.getDriverVersion(), "Driver version must not be null.");
        LOG.info("Driver version: {}", driverVersion);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList();
        databases.add(this.getDefaultDatabase());

        try {
            Connection conn = DriverManager.getConnection(this.defaultUrl, this.username, this.pwd);
            Throwable var3 = null;

            try {
                PreparedStatement ps = conn.prepareStatement("select username from dba_users \nwhere DEFAULT_TABLESPACE <> 'SYSTEM' and DEFAULT_TABLESPACE <> 'SYSAUX' \norder by username");
                ResultSet rs = ps.executeQuery();

                while(rs.next()) {
                    String dbName = rs.getString(1);
                    if (!builtinDatabases.contains(dbName)) {
                        databases.add(dbName);
                    }
                }

                return databases;
            } catch (Throwable var16) {
                var3 = var16;
                throw var16;
            } finally {
                if (conn != null) {
                    if (var3 != null) {
                        try {
                            conn.close();
                        } catch (Throwable var15) {
                            var3.addSuppressed(var15);
                        }
                    } else {
                        conn.close();
                    }
                }

            }
        } catch (Exception var18) {
            throw new CatalogException(String.format("Failed listing database in catalog %s", this.getName()), var18);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (this.listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), (String)null);
        } else {
            throw new DatabaseNotExistException(this.getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!this.databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.getName(), databaseName);
        } else {
            try {
                Connection conn = DriverManager.getConnection(this.defaultUrl, this.username, this.pwd);
                Throwable var3 = null;

                try {
                    List<String> tables = new ArrayList();
                    PreparedStatement ps = conn.prepareStatement("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '" + this.username + "'");
                    ResultSet rs = ps.executeQuery();

                    while(rs.next()) {
                        System.out.println("rs = " + databaseName + "." + rs.getString(1));
                        tables.add(databaseName + "." + rs.getString(1));
                    }

                    return tables;
                } catch (Throwable var17) {
                    var3 = var17;
                    throw var17;
                } finally {
                    if (conn != null) {
                        if (var3 != null) {
                            try {
                                conn.close();
                            } catch (Throwable var16) {
                                var3.addSuppressed(var16);
                            }
                        } else {
                            conn.close();
                        }
                    }

                }
            } catch (Exception var19) {
                var19.printStackTrace();
                throw new CatalogException(String.format("Failed listing database in catalog %s", this.getName()), var19);
            }
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        String defaultDatabase = this.getDefaultDatabase();
        String[] split = tablePath.getObjectName().split("\\.");
        PostgresTablePath pgPath = PostgresTablePath.fromFlinkTableName(tablePath.getObjectName());
        String dbUrl = this.baseUrl + tablePath.getDatabaseName();

        try {
            Connection conn = DriverManager.getConnection(dbUrl, this.username, this.pwd);
            Throwable var7 = null;

            try {
                DatabaseMetaData metaData = conn.getMetaData();
                Optional<UniqueConstraint> primaryKey = this.getPrimaryKey(metaData, split[0], split[1]);
                PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM %s", tablePath.getObjectName()));
                ResultSetMetaData rsmd = ps.getMetaData();
                String[] names = new String[rsmd.getColumnCount()];
                DataType[] types = new DataType[rsmd.getColumnCount()];

                for(int i = 1; i <= rsmd.getColumnCount(); ++i) {
                    names[i - 1] = rsmd.getColumnName(i);
                    types[i - 1] = OracleTypeMapper.mapping((ObjectPath)null, rsmd, i);
                    if (rsmd.isNullable(i) == 0) {
                        types[i - 1] = (DataType)types[i - 1].notNull();
                    }
                }

                Builder tableBuilder = (new Builder()).fields(names, types);
                primaryKey.ifPresent((pk) -> {
                    tableBuilder.primaryKey(pk.getName(), (String[])pk.getColumns().toArray(new String[0]));
                });
                TableSchema tableSchema = tableBuilder.build();
                Map<String, String> props = new HashMap();
                props.put(FactoryUtil.CONNECTOR.key(), "jdbc");
                props.put(JdbcDynamicTableFactory.URL.key(), dbUrl);
                props.put(JdbcDynamicTableFactory.TABLE_NAME.key(), tablePath.getObjectName());
                props.put(JdbcDynamicTableFactory.USERNAME.key(), this.username);
                props.put(JdbcDynamicTableFactory.PASSWORD.key(), this.pwd);
                CatalogTableImpl var17 = new CatalogTableImpl(tableSchema, props, "");
                return var17;
            } catch (Throwable var27) {
                var7 = var27;
                throw var27;
            } finally {
                if (conn != null) {
                    if (var7 != null) {
                        try {
                            conn.close();
                        } catch (Throwable var26) {
                            var7.addSuppressed(var26);
                        }
                    } else {
                        conn.close();
                    }
                }

            }
        } catch (Exception var29) {
            var29.printStackTrace();
            throw new CatalogException(String.format("Failed getting table %s", tablePath.getObjectName()), var29);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List tables = null;

        try {
            tables = this.listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException var6) {
            var6.printStackTrace();
            return false;
        }

        String tableName = tablePath.getFullName().split("\\.")[1];
        Iterator var4 = tables.iterator();

        String table;
        do {
            if (!var4.hasNext()) {
                return false;
            }

            table = (String)var4.next();
        } while(!tableName.equals(table));

        return true;
    }
}
