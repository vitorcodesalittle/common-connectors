package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.connector.SourceConnector;
import br.com.vilmasoftware.connector.SourceRequest;
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

public class SourceConnectorImpl implements SourceConnector {
	protected final DataSource dataSource;

	public SourceConnectorImpl(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	private Column[] getColumns(ResultSet rs) throws SQLException {
		var metadata = rs.getMetaData();
		List<Column> cols = new ArrayList<>();
		for (int i = 0; i < metadata.getColumnCount(); i++) {
			cols.add(new Column(metadata.getColumnName(i + 1), metadata.getColumnTypeName(i + 1)));
		}
		return cols.toArray(new Column[0]);
	}

	public SourceConnectionResult write(SourceRequest request) throws IOException, SQLException {
		final String tableName = "%s.%s".formatted(request.getSchemaName(), request.getTableName());
		final long timeId = System.currentTimeMillis();

		// Write table contents CSV
		final Path csvFile = Files.createTempFile(tableName, "_%d".formatted(timeId));
		final String sqlStr = request.getQuery() != null ? request.getQuery() : "SELECT * FROM %s".formatted(tableName);

		SourceConnectionResult result = new SourceConnectionResult();
		try (Writer writer = Files.newBufferedWriter(csvFile)) {
			CSVLineWriter csvLineWriter = new CSVLineWriter("|", writer);
			long rowCount = 0;

			// Safe as long as not user facing.
			//noinspection SqlSourceToSinkFlow
			try (Connection connection = dataSource.getConnection();
				 PreparedStatement statement = connection.prepareStatement(sqlStr);
				 ResultSet myResultSet = statement.executeQuery()) {
				while (myResultSet.next()) {
					if (rowCount == 0) {
						Column[] columns = getColumns(myResultSet);
						String[] columnNames = Arrays.stream(columns).map(Column::getColumnName).toList().toArray(new String[0]);

						csvLineWriter.writeRow(columnNames);
						String[] dataTypes = Arrays.stream(columns).map(Column::getDataType).toList().toArray(new String[0]);

						// Write the dictionary CSV
						Path dictCsvFile = Files.createTempFile(tableName, "_dict_%d.csv".formatted(timeId));
						try (Writer writer2 = Files.newBufferedWriter(dictCsvFile)) {
							CSVLineWriter csvLineWriter2 = new CSVLineWriter("|", writer2);
							// Write table content into CSV
							csvLineWriter2.writeRow(dataTypes);
						}
						result.setCsvTableDataTypeDictionary(dictCsvFile.toFile());

						// Write table content into CSV
						csvLineWriter.writeRow(columnNames);
					}
					writeRow(myResultSet, csvLineWriter);
					rowCount++;
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			result.setRowCount(rowCount);
		}
		result.setCsvTableContent(csvFile.toFile());
		return result;
	}

	private void writeRow(ResultSet resultSet, CSVLineWriter writer)
			throws SQLException, IOException {
		writer.writeRow(collectRow(resultSet));
	}

	private Object[] collectRow(ResultSet resultSet) throws SQLException {
		Object[] objs = new Object[resultSet.getMetaData().getColumnCount()];
		for (int i = 0; i < objs.length; i++) {
			objs[i] = resultSet.getObject(i + 1);
		}
		return objs;
	}

	@AllArgsConstructor
	@Getter
	private static class Column {
		private String columnName;
		private String dataType;
	}

}
