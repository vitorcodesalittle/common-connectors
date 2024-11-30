package br.com.vilmasoftware.connector;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import br.com.vilmasoftware.readers.AWSFileReader;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

public class SourceConnectorTest {
    private static DataSource dataSource;

    @Test
    @SneakyThrows
    public void testSourceConnectorPostgres() {
        SourceConnector sourceConnector = mockSourceConnector();
        sourceConnector.write(SourceRequest.fromFileAsStream("./etl.json"))
                .parallel()
                .forEach(System.out::println);
        AWSFileReader reader = new AWSFileReader(awsBucket, awsRegion);
        System.out.println(reader.listFiles(awsBucket, ""));
    }

    @BeforeAll
    @SneakyThrows
    public static void setup() {
        dataSource = mockDataSource();
        Process p = new ProcessBuilder("./prepare-test.sh")
                .start();
        p.waitFor(2L, TimeUnit.MINUTES);
        if (p.exitValue() != 0) {
            p.getErrorStream().transferTo(System.err);
            throw new RuntimeException("Não foi possível preparar os DBs de teste");
        }
    }

    private static String dataSourceUrl = "jdbc:postgresql://localhost:5432/Adventureworks";
    private static String user = "postgres";
    private static String password= "postgres";
    private static String awsBucket = "operai-1asd818d3818d3d18";
    private static String awsRegion = "us-east-1";

    @SneakyThrows
    private static SourceConnector mockSourceConnector() {
        return SourceConnector.builder()
            .dataSourceUrl(dataSourceUrl)
            .user(user)
            .password(password)
            .awsBucket(awsBucket)
            .awsRegionName(awsRegion)
            .build();
    }
    private static DataSource mockDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUser(user);
        ds.setUrl(dataSourceUrl);
        ds.setPassword(password);
        return ds;
    }

}
