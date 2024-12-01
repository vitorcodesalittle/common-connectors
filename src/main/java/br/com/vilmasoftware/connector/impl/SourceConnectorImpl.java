package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.connector.SourceConnector;
import br.com.vilmasoftware.connector.SourceRequest;
import br.com.vilmasoftware.connector.TableResolver;
import br.com.vilmasoftware.writers.AWSFileWriter;
import br.com.vilmasoftware.writers.CSVLineWriter;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class SourceConnectorImpl implements SourceConnector {
    protected final DataSource dataSource;
    private final String awsBucketName;
    private final String awsRegionName;
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);

    public SourceConnectorImpl(DataSource dataSource, String awsBucketName, String awsRegionName) {
        this.dataSource = dataSource;
        this.awsBucketName = awsBucketName;
        this.awsRegionName = awsRegionName;
    }

    public Stream<SourceConnectionResult> write(Stream<SourceRequest> request, TableResolver tableResolver) {
        return request.map((req) -> executorService.submit(() -> {
                    try {
                        return write(req, tableResolver);
                    } catch (SQLException | IOException exception) {
                        return new SourceConnectionResult(exception);
                    }
                }))
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        return new SourceConnectionResult(e);
                    }
                });
    }

    public SourceConnectionResult write(SourceRequest request, TableResolver tableResolver) throws IOException, SQLException {
        final long timeId = System.currentTimeMillis();
        final String tableName = "%s.%s".formatted(request.getSchemaName(), request.getTableName());

        // Write table contents CSV
        final Path csvPath = Files.createTempFile(tableName, "_%d.csv".formatted(timeId));
        final Path dictCsvPath = Files.createTempFile(tableName, "_dict_%d.csv".formatted(timeId));
        final String sqlStr = request.getQuery() != null ? request.getQuery() : "SELECT * FROM %s".formatted(tableName);

        long rowCount = 0;
        try (Writer writer = Files.newBufferedWriter(csvPath)) {
            final CSVLineWriter csvLineWriter = new CSVLineWriter("|", writer);
            // Safe as long as not user facing.
            // noinspection SqlSourceToSinkFlow
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(sqlStr);
                 ResultSet myResultSet = statement.executeQuery()) {
                while (myResultSet.next()) {
                    if (rowCount == 0) {
                        final Column[] columns = getColumns(myResultSet);
                        final String[] columnNames = Arrays.stream(columns).map(Column::getColumnName).toList()
                                .toArray(new String[0]);

                        csvLineWriter.writeRow(columnNames);
                        final String[] dataTypes = Arrays.stream(columns).map(Column::getDataType).toList()
                                .toArray(new String[0]);

                        // Write the dictionary CSV
                        try (Writer writer2 = Files.newBufferedWriter(dictCsvPath)) {
                            final CSVLineWriter csvLineWriter2 = new CSVLineWriter("|", writer2);
                            // Write table content into CSV
                            csvLineWriter2.writeRow(dataTypes);
                        }
                    }
                    writeRow(myResultSet, csvLineWriter);
                    rowCount++;
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to write table %s".formatted(tableName), e);
            }
        }
        // Upload to s3
        final AWSFileWriter awsFileWriter = new AWSFileWriter(awsBucketName, awsRegionName);
        awsFileWriter.write(dictCsvPath.toFile(), tableResolver.getDataTypeDictObjectKey(request.getSchemaName(), request.getTableName()));
        awsFileWriter.write(csvPath.toFile(), tableResolver.getObjectKey(request.getSchemaName(), request.getTableName()));
        Files.delete(dictCsvPath);
        Files.delete(csvPath);
        return new SourceConnectionResult(rowCount);
    }

    private void writeRow(final ResultSet resultSet, final CSVLineWriter writer) throws SQLException, IOException {
        writer.writeRow(collectRow(resultSet));
    }

    private Object[] collectRow(final ResultSet resultSet) throws SQLException {
        final Object[] objs = new Object[resultSet.getMetaData().getColumnCount()];
        for (int i = 0; i < objs.length; i++) {
            objs[i] = resultSet.getObject(i + 1);
        }
        return objs;
    }

    private Column[] getColumns(final ResultSet rs) throws SQLException {
        final var metadata = rs.getMetaData();
        final List<Column> cols = new ArrayList<>();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            cols.add(new Column(metadata.getColumnName(i + 1), metadata.getColumnTypeName(i + 1)));
        }
        return cols.toArray(new Column[0]);
    }

    @AllArgsConstructor
    @Getter
    private static class Column {
        private String columnName;
        private String dataType;
    }

}
