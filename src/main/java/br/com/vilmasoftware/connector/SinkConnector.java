package br.com.vilmasoftware.connector;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public interface SinkConnector {
    void write(String schema, String table) throws SQLException, IOException;

    static SinkConnectorBuilder builder() {
        return new SinkConnectorBuilder();
    }
}
