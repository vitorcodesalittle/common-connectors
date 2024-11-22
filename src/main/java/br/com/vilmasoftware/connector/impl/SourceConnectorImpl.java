package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.connector.SourceConnector;
import br.com.vilmasoftware.connector.SourceRequest;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SourceConnectorImpl implements SourceConnector {
	protected final DataSource dataSource;

	public SourceConnectorImpl(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public Column[] getColumnNames(ResultSet rs) throws SQLException {
		var metadata = rs.getMetaData();
		List<Column> cols = new ArrayList<>();
		for (int i = 0; i < metadata.getColumnCount(); i++) {
			cols.add(new Column(metadata.getColumnName(i + 1), metadata.getColumnTypeName(i + 1)));
		}
		return cols.toArray(new Column[0]);
	}

	public SourceConnectionResult write(SourceRequest request) throws IOException, SQLException {
        String schema = request.getSchemaName();
        String table = request.getTableName();
        String query = request.getQuery();
		SourceConnectionResult result = new SourceConnectionResult();
		final long timeId = System.currentTimeMillis();
		String tableName = "%s.%s".formatted(schema, table);

		// Write table contents CSV
		Path csvFile = Files.createTempFile(tableName, "_%d".formatted(timeId));
		try (Writer writer = Files.newBufferedWriter(csvFile)) {
			CSVLineWriter csvLineWriter = new CSVLineWriter("|", writer);
			long rowCount = 0;
			String sqlStr = query != null ? query : "SELECT * FROM %s".formatted(tableName);
			Column[] columns;
			String[] columnNames = null;
			String[] dataTypes;
			try (Connection connection = dataSource.getConnection()) {
				PreparedStatement statement = connection.prepareStatement(sqlStr);
				ResultSet myResultSet = statement.executeQuery();
				while (myResultSet.next()) {
					if (rowCount == 0) {
						columns = getColumnNames(myResultSet);
						columnNames = Arrays.stream(columns).map(Column::getColumnName).toList().toArray(new String[0]);

						csvLineWriter.writeRow(columnNames);
						dataTypes = Arrays.stream(columns).map(Column::getDataType).toList().toArray(new String[0]);

						// Write the dictionary CSV
						Path dictCsvFile = Files.createTempFile(tableName, "_dict_%d".formatted(timeId));
						try (Writer writer2 = Files.newBufferedWriter(dictCsvFile)) {
							CSVLineWriter csvLineWriter2 = new CSVLineWriter("|", writer2);
							// Write table content into CSV
							csvLineWriter2.writeRow(dataTypes);
						}
						result.setCsvTableDataTypeDictionary(dictCsvFile.toFile());

						// Write table content into CSV
						csvLineWriter.writeRow(columnNames);
					}
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

	private void writeRow(ResultSet resultSet, String[] columnNames, CSVLineWriter writer)
			throws SQLException, IOException {
		Object[] objs = new Object[columnNames.length];
		collectRow(resultSet, objs);
		writer.writeRow(objs);
	}

	private void collectRow(ResultSet resultSet, Object[] objs) throws SQLException {
		for (int i = 0; i < objs.length; i++) {
			objs[i] = resultSet.getObject(i + 1);
		}
	}

	@AllArgsConstructor
	@Getter
	public static class Column {
		private String columnName;
		private String dataType;
	}

}
