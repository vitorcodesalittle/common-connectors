package br.com.vilmasoftware.connector;

public interface TableResolver {

    /**
     * Retrieves the key to the object representation in the
     * external object store.
     *
     * Example:
     *  A custom S3 TableResolver implementation will retrieve a S3 object key
     *  such as prefix//schema//tableName.csv
     *
     * @param schemaName
     * @param tableName
     * @return
     */
    String getObjectKey(String schemaName, String tableName);

    /**
     * Retrieves the key object data types dictionary in the
     * external object store.
     *
     * Example:
     *  A custom S3 URI TableResolver implementation will retrieve a S3 object key
     *  such as prefix//schema//tableName.dict.csv
     *
     * @param schemaName
     * @param tableName
     * @return
     */
    String getDataTypeDictObjectKey(String schemaName, String tableName);

}
