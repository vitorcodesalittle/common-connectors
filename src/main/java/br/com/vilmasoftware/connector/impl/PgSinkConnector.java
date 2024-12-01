package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.*;
import br.com.vilmasoftware.readers.AWSFileReader;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class PgSinkConnector implements SinkConnector {
    private final DataSource dataSource;
    private final String awsBucketName;
    private final String awsRegionName;
    private final ExecutorService executorService;

    public PgSinkConnector(DataSource dataSource, String awsBucketName, String awsRegionName, ExecutorService executorService) {
        assert executorService != null;
        this.executorService = executorService;
        this.dataSource = dataSource;
        this.awsBucketName = awsBucketName;
        this.awsRegionName = awsRegionName;
    }

    @Override
    public SinkConnectionResult write(SinkRequest request, TableResolver tableResolver) {
        try (Connection conn = dataSource.getConnection()) {
            final AWSFileReader awsFileWriter = new AWSFileReader(awsBucketName, awsRegionName);
            final String objectKey = tableResolver.getObjectKey(request.getSchemaName(), request.getTableName());
            final File csvfile = awsFileWriter.read(objectKey);
            final CopyManager copyManager = new CopyManager((BaseConnection) conn);
            try (FileReader fileReader = new FileReader(csvfile)) {
                long rowsCount = copyManager.copyIn("COPY %s.%s FROM STDIN WITH (NULL 'NULL', FORMAT csv, HEADER, DELIMITER '|')".formatted(
                        request.getSchemaName(),
                        request.getTableName()), fileReader);
                return new SinkConnectionResult(rowsCount);
            } catch (IOException | SQLException e) {
                return new SinkConnectionResult(e);
            }
        } catch (IOException | SQLException e) {
            return new SinkConnectionResult(e);
        }
    }

    @Override
    public Stream<SinkConnectionResult> write(Stream<SinkRequest> requests, TableResolver tableResolver) throws SQLException, IOException {
        return requests.map(request -> executorService.submit(() -> write(request, tableResolver)))
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        return new SinkConnectionResult(e);
                    }
                });
    }
}


