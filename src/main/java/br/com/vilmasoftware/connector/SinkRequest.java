package br.com.vilmasoftware.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

@Getter
public class SinkRequest {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private String tableName;
    private String schemaName;

    public SinkRequest(String schemaName, String tableName) {
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public SinkRequest() {
    }

    public static Stream<SinkRequest> fromFileAsStream(String string) {
        try {
            return objectMapper.readValue(Path.of(string).toFile(), new TypeReference<List<SourceRequest>>() {
                    })
                    .stream()
                    .map(sourceRequest -> new SinkRequest(sourceRequest.getSchemaName(), sourceRequest.getTableName()));
        } catch (IOException e) {
            throw new UncheckedIOException("Erro ao ler JSON de ETL", e);
        }
    }

}
