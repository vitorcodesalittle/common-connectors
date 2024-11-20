package br.com.vilmasoftware.connector.writers;

import java.io.IOException;
import java.io.Writer;

public class CSVLineWriter {
    private final String delimiter;
    private final String rowDelimiter;

    private final Writer writer;
    private final String nullString = "NULL";

    public CSVLineWriter(String delimiter, Writer writer) {
        this.delimiter = delimiter;
        this.rowDelimiter = System.lineSeparator();
        this.writer = writer;
    }

    public int writeRow(Object[] row) throws IOException {
        for (int i = 0; i < row.length; i++) {
            Object obj = row[i];
            writer.write(parse(obj));
            if (i != row.length - 1) {
                writer.write(delimiter);
            }
        }
        writer.write(rowDelimiter);
        return delimiter.length() + rowDelimiter.length();
    }
    private String parse(Object object) {
        if (object == null) {
            return nullString;
        }
        if (object instanceof String) {
            return "\"%s\"".formatted(object);
        }
        return object.toString();
    }

}
