package br.com.vilmasoftware.connector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;

@Getter
public class SourceRequest {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private String tableName;
    private String schemaName;
    private String query;

    public SourceRequest(String schemaName, String tableName, String query) {
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.query = query;
    }

    public SourceRequest() {
    }

    public static Stream<SourceRequest> fromFileAsStream(String string) {
        try {
            return objectMapper.readValue(Path.of(string).toFile(), new TypeReference<List<SourceRequest>>() {
            }).stream();
        } catch (IOException e) {
            throw new UncheckedIOException("Erro ao ler JSON de ETL", e);
        }
    }
}
