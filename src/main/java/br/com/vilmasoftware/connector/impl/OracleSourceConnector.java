package br.com.vilmasoftware.connector.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OracleSourceConnector extends BaseSourceConnector {
    protected DataSource dataSource;

    public OracleSourceConnector(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Column[] getColumnNames(String schema, String entity) throws SQLException {
        String query = "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ?".stripIndent();
        List<Column> columns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, schema.toUpperCase());
            stmt.setString(2, entity.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");

                    columns.add(new Column(columnName, dataType));
                }
            }
        }

        return columns.toArray(new Column[0]);
    }
}
