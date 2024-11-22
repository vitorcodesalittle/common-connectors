package br.com.vilmasoftware.connector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Getter
public class SourceRequest {
    private String tableName;
    private String schemaName;
    private String query;
}
