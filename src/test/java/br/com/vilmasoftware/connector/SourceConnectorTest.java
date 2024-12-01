package br.com.vilmasoftware.connector;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import lombok.SneakyThrows;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SourceConnectorTest {

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
                .executorService(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4))
                .build();
    }

    private static DataSource mockDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUser(TestConfig.user);
        ds.setUrl(TestConfig.sourceDataSourceUrl);
        ds.setPassword(TestConfig.password);
        return ds;
    }

    @Test
    @SneakyThrows
    public void testSourceConnectorPostgres() {
        SourceConnector sourceConnector = mockSourceConnector();
        Assertions.assertEquals(1, sourceConnector.write(SourceRequest.fromFileAsStream("./el.json"),
                        TestConfig.s3SimpleTableResolver)
                .parallel()
                .filter(result -> result.getException() != null)
                .peek(System.out::println)
                .toList()
                .size());
    }

}
