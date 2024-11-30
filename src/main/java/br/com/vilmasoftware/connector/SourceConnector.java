package br.com.vilmasoftware.connector;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public interface SourceConnector {
    SourceConnectionResult write(SourceRequest request) throws IOException, SQLException;
    Stream<SourceConnectionResult> write(Stream<SourceRequest> request);

    static SourceConnectorBuilder builder() {
        return new SourceConnectorBuilder();
    }
}
