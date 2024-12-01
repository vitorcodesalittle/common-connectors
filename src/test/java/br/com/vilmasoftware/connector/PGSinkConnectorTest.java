package br.com.vilmasoftware.connector;

import br.com.vilmasoftware.readers.AWSFileReader;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

public class PGSinkConnectorTest {
    @Test
    @SneakyThrows
    public void testPostgresSinkConnector() {
        SinkConnector sourceConnector = mockSinkConnector();
        sourceConnector.write(SinkRequest.fromFileAsStream("./el.json"),
                        TestConfig.s3SimpleTableResolver)
                .parallel()
                .forEach(System.out::println);
        AWSFileReader reader = new AWSFileReader(TestConfig.awsBucket, TestConfig.awsRegion);
        System.out.println(reader.listFiles(TestConfig.awsBucket, ""));
    }

    @BeforeAll
    @SneakyThrows
    public static void setup() {
//        Process p = new ProcessBuilder("./prepare-test.sh")
//                .start();
//        p.waitFor(2L, TimeUnit.MINUTES);
//        if (p.exitValue() != 0) {
//            p.getErrorStream().transferTo(System.err);
//            throw new RuntimeException("Não foi possível preparar os DBs de teste");
//        }
    }

    @SneakyThrows
    private static SinkConnector mockSinkConnector() {
        return SinkConnector.builder()
                .dataSourceUrl(TestConfig.sinkDataSourceUrl)
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
