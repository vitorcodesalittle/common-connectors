package br.com.vilmasoftware.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

@Getter
public class SinkRequest {
    private String tableName;
    private String schemaName;
    public SinkRequest(String schemaName, String tableName) {
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public SinkRequest() {
    }

}
