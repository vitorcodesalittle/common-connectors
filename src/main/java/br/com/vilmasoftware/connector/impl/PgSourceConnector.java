package br.com.vilmasoftware.connector.impl;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PgSourceConnector extends BaseSourceConnector {
    protected DataSource dataSource;
    public PgSourceConnector(DataSource dataSource) {
            super(dataSource);
    }

    @Override
    public Column[] getColumnNames(String schema, String entity) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            final PreparedStatement statement = connection.prepareStatement("""
                       SELECT column_name, data_type
                       FROM information_schema.COLUMNS
                       WHERE table_schema = ?
                       AND table_name = ?
                       ORDER BY ordinal_position;
                    """);
            statement.setString(1, schema);
            statement.setString(2, entity);
            ResultSet myResultSet = statement.executeQuery();
            List<Column> columnNames = new ArrayList<>();
            while (myResultSet.next()) {
                String columnName = myResultSet.getString(1);
                String dataType = myResultSet.getString(2);
                columnNames.add(new Column(columnName, dataType));
            }
            return columnNames.toArray(new Column[0]);
        }
    }
}

