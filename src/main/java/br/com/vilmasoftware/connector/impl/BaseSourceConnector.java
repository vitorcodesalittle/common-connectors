package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.connector.SourceConnector;
import br.com.vilmasoftware.writers.CSVLineWriter;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public abstract class BaseSourceConnector implements SourceConnector {
    protected final DataSource dataSource;
    public BaseSourceConnector(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    abstract public Column[] getColumnNames(String schema, String entity) throws SQLException;
    public SourceConnectionResult write(String schema, String entity) throws IOException, SQLException {
        SourceConnectionResult result = new SourceConnectionResult();
        Column[] columns = getColumnNames(schema, entity);
        String[] columnNames = Arrays.stream(columns).map(Column::getColumnName).toList().toArray(new String[0]);
        String[] dataTypes = Arrays.stream(columns).map(Column::getDataType).toList().toArray(new String[0]);
        final long timeId = System.currentTimeMillis();
        String tableName = "%s.%s".formatted(schema, entity);

        // Write the dictionary CSV
        Path dictCsvFile = Files.createTempFile(tableName, "_dict_%d".formatted(timeId));
        try (Writer writer = Files.newBufferedWriter(dictCsvFile)) {
            CSVLineWriter csvLineWriter = new CSVLineWriter("|", writer);
            // Write table content into CSV
            writeHeader(columnNames, csvLineWriter);
            csvLineWriter.writeRow(dataTypes);
        }
        result.setCsvTableDataTypeDictionary(dictCsvFile.toFile());
        // Write table contents CSV
        Path csvFile = Files.createTempFile(tableName, "_%d".formatted(timeId));
        try (Writer writer = Files.newBufferedWriter(csvFile)) {
            CSVLineWriter csvLineWriter = new CSVLineWriter("|", writer);
            // Write table content into CSV
            writeHeader(columnNames, csvLineWriter);
            long rowCount = 0;
            String sqlStr = "SELECT * FROM %s".formatted(tableName);
            try (Connection connection = dataSource.getConnection()) {
                PreparedStatement statement = connection.prepareStatement(sqlStr);
                ResultSet myResultSet = statement.executeQuery();
                while (myResultSet.next()) {
                    writeRow(myResultSet, columnNames, csvLineWriter);
                    rowCount++;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            result.setRowCount(rowCount);
        }
        File tableDumpFile = csvFile.toFile();
        result.setCsvTableContent(tableDumpFile);
        return result;
    }

    private void writeHeader(String[] columnNames, CSVLineWriter writer) throws IOException {
        writer.writeRow(columnNames);
    }

    private void writeRow(ResultSet resultSet, String[] columnNames, CSVLineWriter writer) throws SQLException, IOException {
        Object[] objs = new Object[columnNames.length];
        collectRow(resultSet, objs);
        writer.writeRow(objs);
    }



    private void collectRow(ResultSet resultSet, Object[] objs) throws SQLException {
        for (int i = 0; i < objs.length; i++) {
            objs[i] = resultSet.getObject(i+1);
        }
    }

    @AllArgsConstructor
    @Getter
    public static class Column {
        private String columnName;
        private String dataType;
    }
}
