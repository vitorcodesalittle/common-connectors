package br.com.vilmasoftware.connector.writers;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;

@RequiredArgsConstructor
public class AWSFileWriter {
    private final AWSCredentials awsCredentials;
    private final String bucket;
    private final String region;
    private S3Client s3Client = null;

    public void write(File file, String objectKey) {
        if (s3Client == null) {
            s3Client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials()))
                    .build();
        }
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        s3Client.putObject(request, RequestBody.fromFile(file));
    }
    private AwsBasicCredentials credentials() {
        return AwsBasicCredentials
                .create(awsCredentials.getAccessKeyId(), awsCredentials.getSecretKey());
    }
}
