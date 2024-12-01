package br.com.vilmasoftware.cli;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public
enum OptionKey {
    FILE("file"),
    USER("user"),
    PASSWORD("password"),
    DATASOURCE_URL("datasource-url"),
    AWS_BUCKET("aws-bucket"),
    AWS_REGION("aws-region"),
    TYPE("type");
    private String key;

    public String key() {
        return key;
    }
}
