package br.com.vilmasoftware;

import br.com.vilmasoftware.connector.SinkConnector;
import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.connector.SourceConnector;
import br.com.vilmasoftware.connector.SourceRequest;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import br.com.vilmasoftware.connector.impl.PgSinkConnector;
import br.com.vilmasoftware.connector.impl.SourceConnectorImpl;
import br.com.vilmasoftware.readers.AWSFileReader;
import br.com.vilmasoftware.writers.AWSFileWriter;

import lombok.Getter;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.commons.cli.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        runSourceConnectorExample(args);
    }


    private static void runSourceConnectorExample(String[] args) {
        var options = new Options();
        setAwsOptions(options);
        setDataSourceOptions(options);

        CommandLineParser parser = new DefaultParser();
        CommandLine cli;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        // Run Source Connector
        final String schema = cli.getOptionValue("schemaName");

        try {
            SourceConnector sourceConnector = createSourceConnector(cli.getOptionValue("dataSourceUrl"), cli.getOptionValue("user"), cli.getOptionValue("password"));
            for (String tableName : cli.getOptionValue("tableNames").split(",")) {
                SourceConnectionResult result = sourceConnector.write(new SourceRequest(schema, tableName));
                // Upload to s3
                AWSFileWriter awsFileWriter = new AWSFileWriter(
                        cli.getOptionValue("awsS3Bucket"),
                        cli.getOptionValue("awsRegion")
                );
                awsFileWriter.write(result.getCsvTableDataTypeDictionary(), "%s_dict.csv".formatted(tableName));
                awsFileWriter.write(result.getCsvTableContent(), "%s.csv".formatted(tableName));
            }
        } catch (IOException | SQLException |  NotImplementedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runSinkConnectorExample(String[] args) {
        var options = new Options();
        setAwsOptions(options);
        setDataSourceOptions(options);
        CommandLineParser parser = new DefaultParser();
        CommandLine cli;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        try {
            // Run Source Connector
            SinkConnector sinkConnector = createSinkConnector(cli.getOptionValue("dataSourceUrl"), cli.getOptionValue("user"), cli.getOptionValue("password"));
            final String schema = cli.getOptionValue("schemaName");
            for (String tableName : cli.getOptionValue("tableNames").split(",")) {

                // Upload to s3
                AWSFileReader awsFileWriter = new AWSFileReader(
                        cli.getOptionValue("awsS3Bucket"),
                        cli.getOptionValue("awsRegion")
                );
                File csvfile = awsFileWriter.read("%s.csv".formatted(tableName));
                sinkConnector.write(schema, tableName, csvfile);
            }
        } catch (IOException | SQLException | NotImplementedException  e) {
            throw new RuntimeException(e);
        }
    }

    public static SinkConnector createSinkConnector(String dataSourceUrl, String user, String password) throws NotImplementedException, SQLException {
        if (dataSourceUrl == null || dataSourceUrl.isEmpty()) {
            throw new IllegalArgumentException("Datasource URL cannot be null or empty");
        }

        try {
            var uri = new URI(dataSourceUrl);
            String scheme = uri.getScheme();

            if (scheme != null) {
                String[] parts = scheme.split(":");
                if (parts.length > 1) {
                    DataSource dataSource;
                    var notImplemented =  new NotImplementedException("%s is not a supported datasource");
                    switch (DataSourceSupportedProviders.byJdbcId(parts[1])
                            .orElseThrow(() -> notImplemented)) {
                        case POSTGRES -> {
                            dataSource = createPostgresDataSource(dataSourceUrl, user, password);
                            return new PgSinkConnector(dataSource);
                        }
                        case ORACLE -> {
                            throw notImplemented;
                        }
                    }

                }
            }
            throw new IllegalArgumentException("Invalid datasource URL format");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid datasource URL", e);
        }
    }
    public static SourceConnector createSourceConnector(String dataSourceUrl, String user, String password) throws NotImplementedException, SQLException {
        if (dataSourceUrl == null || dataSourceUrl.isEmpty()) {
            throw new IllegalArgumentException("Datasource URL cannot be null or empty");
        }

        try {
            var uri = new URI(dataSourceUrl);
            String scheme = uri.getSchemeSpecificPart();
            if (scheme != null) {
                String[] parts = scheme.split(":");
                log.error("parts: {}", Arrays.toString(parts));
                if (parts.length > 0) {
                    var notImplemented =  new NotImplementedException("%s is not a supported datasource");
                    switch (DataSourceSupportedProviders.byJdbcId(parts[0])
                            .orElseThrow(() -> notImplemented)) {
                        case POSTGRES -> {
                            var dataSource = createPostgresDataSource(dataSourceUrl, user, password);
                            return new SourceConnectorImpl(dataSource);
                        }
                        case ORACLE -> {
                            var dataSource = createOracleDataSource(dataSourceUrl, user, password);
                            return new SourceConnectorImpl(dataSource);
                        }
                    }

                }
            }
            throw new IllegalArgumentException("Invalid datasource URL format");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid datasource URL", e);
        }
    }

    private static PGSimpleDataSource createPostgresDataSource(String dataSourceUrl, String user, String password) {
        PGSimpleDataSource postgresDataSource = new PGSimpleDataSource();
        postgresDataSource.setURL(dataSourceUrl);
        postgresDataSource.setUser(user);
        postgresDataSource.setPassword(password);
        return postgresDataSource;
    }

    public static DataSource createOracleDataSource(String url, String user, String password) throws SQLException {
        OracleDataSource oracleDataSource = new OracleDataSource();
        oracleDataSource.setURL(url);
        oracleDataSource.setUser(user);
        oracleDataSource.setPassword(password);

        return oracleDataSource;
    }
    public enum DataSourceSupportedProviders {
        POSTGRES("postgres"), ORACLE("oracle");

        @Getter
        private String jdbcId;

        DataSourceSupportedProviders(String jdbcId) {
            this.jdbcId = jdbcId;
        }

        public static Optional<DataSourceSupportedProviders> byJdbcId(String search) {
            for (DataSourceSupportedProviders d : DataSourceSupportedProviders.values()) {
                if (d.getJdbcId().equals(search)) return Optional.of(d);
            }
            return Optional.empty();
        }
    }
    private static void setDataSourceOptions(Options options) {
        options.addOption(Option.builder().longOpt("dataSourceUrl").hasArg().desc("data source URL for the source connector").build());
        options.addOption(Option.builder().longOpt("schemaName").hasArg().required().desc("schema that will be copied").build());
        options.addOption(Option.builder().longOpt("tableNames").hasArg().required().desc("comma separated list of tables that will be exported to AWS").build());
        options.addOption(Option.builder().longOpt("user").hasArg().desc("").build());
        options.addOption(Option.builder().longOpt("password").hasArg().desc("").build());
    }
    private static void setAwsOptions(Options options) {
        options.addOption(Option.builder().longOpt("awsRegion").hasArg().desc("AWS Region").build());
        options.addOption(Option.builder().longOpt("awsS3Bucket").hasArg().desc("Resource where CSV's will be uploaded").build());
    }
}
