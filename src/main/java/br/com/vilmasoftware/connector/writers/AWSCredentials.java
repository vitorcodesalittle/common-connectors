package br.com.vilmasoftware.connector.writers;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class AWSCredentials {
    private String accessKeyId;
    private String secretKey;
}
