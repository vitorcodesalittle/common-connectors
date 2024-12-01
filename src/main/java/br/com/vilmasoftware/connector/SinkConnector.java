package br.com.vilmasoftware.connector;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public interface SinkConnector {

    SinkConnectionResult write(SinkRequest request, TableResolver tableResolver) throws SQLException, IOException;
    Stream<SinkConnectionResult> write(Stream<SinkRequest> request, TableResolver tableResolver) throws SQLException, IOException;

    static SinkConnectorBuilder builder() {
        return new SinkConnectorBuilder();
    }
}
