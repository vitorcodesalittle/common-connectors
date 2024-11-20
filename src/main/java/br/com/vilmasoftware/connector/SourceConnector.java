package br.com.vilmasoftware.connector;

import java.io.IOException;
import java.sql.SQLException;

public interface SourceConnector {
    SourceConnectionResult write(String schema, String entity) throws IOException, SQLException;
}
