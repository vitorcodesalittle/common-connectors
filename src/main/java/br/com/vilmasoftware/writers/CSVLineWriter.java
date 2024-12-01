package br.com.vilmasoftware.writers;

import java.io.IOException;
import java.io.Writer;

public class CSVLineWriter {
    private final String delimiter;
    private final String rowDelimiter;
    private final Writer writer;
    private String nullString = "NULL";

    public CSVLineWriter(String delimiter, Writer writer) {
        this.delimiter = delimiter;
        this.rowDelimiter = System.lineSeparator();
        this.writer = writer;
    }

    public CSVLineWriter(String delimiter, Writer writer, String nullString) {
        this.delimiter = delimiter;
        this.rowDelimiter = System.lineSeparator();
        this.writer = writer;
        this.nullString = nullString;
    }

    public void writeRow(Object[] row) throws IOException {
        for (int i = 0; i < row.length; i++) {
            writer.write(parse(row[i]));
            if (i != row.length - 1) {
                writer.write(delimiter);
            }
        }
        writer.write(rowDelimiter);
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
