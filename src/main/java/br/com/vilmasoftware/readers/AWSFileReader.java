package br.com.vilmasoftware.readers;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@RequiredArgsConstructor
public class AWSFileReader {
    private final String bucket;
    private final String region;
    private S3Client s3Client = null;


    private S3Client getS3Client() {
        if (s3Client == null) {
            s3Client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
        }
        return s3Client;
    }

    public File read(String objectKey) throws IOException {
        S3Client s3Client = getS3Client();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        Path path = Paths.get(System.getProperty("java.io.tmpdir"), "%d-%s".formatted(System.currentTimeMillis(), objectKey));
        Files.createDirectories(path.getParent());
        s3Client.getObject(request, path);
        return path.toFile();
    }

    public List<String> listFiles(String bucket, String prefix) {
        S3Client s3Client = getS3Client();
        var response = s3Client.listObjects(ListObjectsRequest.builder()
                .prefix(prefix)
                .bucket(bucket)
                .build()
        );
        return response.contents().stream().map(S3Object::key).toList();
    }
}
