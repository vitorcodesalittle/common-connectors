package br.com.vilmasoftware.writers;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;

@RequiredArgsConstructor
public class AWSFileWriter {
    private final String bucket;
    private final String region;
    private S3Client s3Client = null;

    public void write(File file, String objectKey) {
        if (s3Client == null) {
            s3Client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
        }
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        s3Client.putObject(request, RequestBody.fromFile(file));
    }
}
