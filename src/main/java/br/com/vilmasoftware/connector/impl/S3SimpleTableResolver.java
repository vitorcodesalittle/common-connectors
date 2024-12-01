package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.TableResolver;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class S3SimpleTableResolver implements TableResolver {
    private final String prefix;
    @Override
    public String getObjectKey(String schemaName, String tableName) {
        return "%s/%s/%s.csv".formatted(prefix, schemaName, tableName);
    }

    @Override
    public String getDataTypeDictObjectKey(String schemaName, String tableName) {
        return "%s/%s/%s.dict.csv".formatted(prefix, schemaName, tableName);
    }
}
