package br.com.vilmasoftware.connector;

import java.io.IOException;
import java.sql.SQLException;

public interface SourceConnector {
    SourceConnectionResult write(SourceRequest request) throws IOException, SQLException;
}
