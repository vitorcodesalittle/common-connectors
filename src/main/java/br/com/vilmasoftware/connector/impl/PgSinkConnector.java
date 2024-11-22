package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.SinkConnector;
import lombok.RequiredArgsConstructor;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@RequiredArgsConstructor
public class PgSinkConnector implements SinkConnector {
    private final DataSource dataSource;

    @Override
    public void write(String schema, String entity, File file) throws SQLException {
        String tableName = "%s.%s".formatted(schema, entity);
        try (Connection conn = dataSource.getConnection()) {
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            try (FileReader fileReader = new FileReader(file)) {
                copyManager.copyIn("COPY %s FROM STDIN WITH (NULL 'NULL', FORMAT csv, HEADER, DELIMITER '|')".formatted(tableName), fileReader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
