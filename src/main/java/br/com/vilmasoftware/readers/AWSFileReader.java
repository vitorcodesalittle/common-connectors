package br.com.vilmasoftware.readers;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
        // TODO: Create just path, so we dont have to delete
        Path path = Files.createTempFile(objectKey, "");
        Files.deleteIfExists(path);
        s3Client.getObject(request, path);
        return path.toFile();
    }
}
