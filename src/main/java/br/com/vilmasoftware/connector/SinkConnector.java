package br.com.vilmasoftware.connector;

import java.io.File;
import java.sql.SQLException;

public interface SinkConnector {
    void write(String schema, String table, File file) throws SQLException;
}
