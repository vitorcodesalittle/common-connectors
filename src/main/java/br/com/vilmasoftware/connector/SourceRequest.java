package br.com.vilmasoftware.connector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
public class SourceRequest {
    private String tableName;
    private String schemaName;
    private String query;

    public SourceRequest(String tableName, String schemaName) {
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public SourceRequest(String query) {
        this.query = query;
    }
}
