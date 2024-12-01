package br.com.vilmasoftware.cli;

import br.com.vilmasoftware.connector.*;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import br.com.vilmasoftware.connector.impl.S3SimpleTableResolver;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.sql.SQLException;

public class CliConfiguration {
    private final static TableResolver s3SimpleTableResolver = new S3SimpleTableResolver("data/");
    private final String type;
    private final String dataSourceUrl;
    private final String file;
    private final String user;
    private final String password;
    private final String awsBucket;
    private final String awsRegionName;

    public CliConfiguration(String[] args) {
        var cli = getCli(args);
        type = cli.getOptionValue(OptionKey.TYPE.key());
        dataSourceUrl = cli.getOptionValue(OptionKey.DATASOURCE_URL.key());
        user = cli.getOptionValue(OptionKey.USER.key());
        password = cli.getOptionValue(OptionKey.PASSWORD.key());
        awsBucket = cli.getOptionValue(OptionKey.AWS_BUCKET.key());
        awsRegionName = cli.getOptionValue(OptionKey.AWS_REGION.key());
        file = cli.getOptionValue(OptionKey.FILE.key());
        assert dataSourceUrl != null;
        assert type != null;
        assert awsBucket != null;
        assert awsRegionName != null;
        assert file != null;
    }

    private static CommandLine getCli(String[] args) {
        var options = new Options();
        options.addOption(Option.builder()
                .longOpt(OptionKey.TYPE.key())
                .desc("Can be 'sink' or 'source'")
                .required()
                .hasArg()
                .build());
        setAwsOptions(options);
        setDataSourceOptions(options);
        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setDataSourceOptions(Options options) {
        options.addOption(Option.builder().longOpt(OptionKey.DATASOURCE_URL.key()).hasArg()
                .desc("data source URL for the source connector").build());
        options.addOption(Option.builder().longOpt(OptionKey.FILE.key()).hasArg().required()
                .desc("A file with a json array that describes the schema, table and queries.").build());
        options.addOption(Option.builder().longOpt(OptionKey.USER.key()).hasArg().desc("").build());
        options.addOption(Option.builder().longOpt(OptionKey.PASSWORD.key()).hasArg().desc("").build());
    }

    private static void setAwsOptions(Options options) {
        options.addOption(Option.builder().longOpt(OptionKey.AWS_REGION.key()).hasArg().desc("AWS Region").build());
        options.addOption(
                Option.builder().longOpt(OptionKey.AWS_BUCKET.key()).hasArg().desc("Resource where CSV's will be uploaded").build());
    }

    public void run() {
        try {
            if (type.equals("source")) {
                runSourceConnectorExample();
            } else if (type.equals("sink")) {
                runSinkConnectorExample();
            } else {
                throw new IllegalArgumentException("type must be source or sink");
            }
        } catch (NotImplementedException e) {
            System.out.println("Não implementado: " + e.getMessage());
        }
    }

    private void runSourceConnectorExample() throws NotImplementedException {
        System.out.println("Starsting source connector");
        SourceConnector sourceConnector = SourceConnector.builder().dataSourceUrl(dataSourceUrl)
                .user(user)
                .password(password)
                .awsBucket(awsBucket)
                .awsRegionName(awsRegionName)
                .build();
        try (var sourceStream = SourceRequest.fromFileAsStream(file)) {
            sourceConnector.write(sourceStream, s3SimpleTableResolver)
                    .forEach(System.out::println);
            System.out.println("done.");
        }
    }

    private void runSinkConnectorExample() {
        try {
            // Run Sink Connector
            SinkConnector sinkConnector = SinkConnector.builder()
                    .dataSourceUrl(dataSourceUrl)
                    .user(user)
                    .password(awsRegionName)
                    .awsBucket(awsBucket)
                    .awsRegionName(awsRegionName)
                    .build();
            try (var sinkStream = SinkRequest.fromFileAsStream(file)) {
                sinkConnector.write(sinkStream, s3SimpleTableResolver);
            }
        } catch (IOException | SQLException | NotImplementedException e) {
            throw new RuntimeException(e);
        }
    }
}
