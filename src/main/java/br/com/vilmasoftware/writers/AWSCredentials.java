package br.com.vilmasoftware.writers;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class AWSCredentials {
    private String accessKeyId;
    private String secretKey;
}
