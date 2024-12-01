package br.com.vilmasoftware.connector;

import javax.sql.DataSource;

import br.com.vilmasoftware.connector.impl.S3SimpleTableResolver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import br.com.vilmasoftware.readers.AWSFileReader;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

public class SourceConnectorTest {

    @Test
    @SneakyThrows
    public void testSourceConnectorPostgres() {
        SourceConnector sourceConnector = mockSourceConnector();
        sourceConnector.write(SourceRequest.fromFileAsStream("./etl.json"),
                        TestConfig.s3SimpleTableResolver)
                .parallel()
                .forEach(System.out::println);
        AWSFileReader reader = new AWSFileReader(TestConfig.awsBucket, TestConfig.awsRegion);
        System.out.println(reader.listFiles(TestConfig.awsBucket, ""));
    }

    @BeforeAll
    @SneakyThrows
    public static void setup() {
        Process p = new ProcessBuilder("./prepare-test.sh")
                .start();
        p.waitFor(2L, TimeUnit.MINUTES);
        if (p.exitValue() != 0) {
            p.getErrorStream().transferTo(System.err);
            throw new RuntimeException("Não foi possível preparar os DBs de teste");
        }
    }


    @SneakyThrows
    private static SourceConnector mockSourceConnector() {
        return SourceConnector.builder()
            .dataSourceUrl(TestConfig.sourceDataSourceUrl)
            .user(TestConfig.user)
            .password(TestConfig.password)
            .awsBucket(TestConfig.awsBucket)
            .awsRegionName(TestConfig.awsRegion)
            .build();
    }
    private static DataSource mockDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUser(TestConfig.user);
        ds.setUrl(TestConfig.sourceDataSourceUrl);
        ds.setPassword(TestConfig.password);
        return ds;
    }

}
