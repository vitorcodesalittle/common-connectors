package br.com.vilmasoftware.readers;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@RequiredArgsConstructor
public class AWSFileReader {
    private final String bucket;
    private final String region;
    private S3Client s3Client = null;

    public File read(String objectKey) throws IOException {
        if (s3Client == null) {
            s3Client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
        }
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        Path path = Paths.get(System.getProperty("java.io.tmpdir"), "%s_%d".formatted(objectKey, System.currentTimeMillis()));
        s3Client.getObject(request, path);
        return path.toFile();
    }
}
