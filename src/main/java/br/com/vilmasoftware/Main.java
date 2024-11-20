package br.com.vilmasoftware;

import br.com.vilmasoftware.connector.SinkConnector;
import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.connector.SourceConnector;
import br.com.vilmasoftware.connector.impl.PgSinkConnector;
import br.com.vilmasoftware.connector.impl.PgSourceConnector;
import br.com.vilmasoftware.connector.readers.AWSFileReader;
import br.com.vilmasoftware.connector.writers.AWSCredentials;
import br.com.vilmasoftware.connector.writers.AWSFileWriter;

import org.apache.commons.cli.*;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        runSinkConnectorExample(args);
    }

    private static void runSourceConnectorExample(String[] args) {
        var options = new Options();

        // Parse CLI Options
        options.addOption(Option.builder().longOpt("dataSourceUrl").hasArg().desc("data source URL for the source connector").build());
        options.addOption(Option.builder().longOpt("schemaName").hasArg().required().desc("schema that will be copied").build());
        options.addOption(Option.builder().longOpt("tableNames").hasArg().required().desc("comma separated list of tables that will be exported to AWS").build());
        options.addOption(Option.builder().longOpt("user").hasArg().desc("").build());
        options.addOption(Option.builder().longOpt("password").hasArg().desc("").build());

        options.addOption(Option.builder().longOpt("awsAccessKeyId").hasArg().desc("AWS Access Key ID").build());
        options.addOption(Option.builder().longOpt("awsSecretKey").hasArg().desc("AWS Secret Key").build());
        options.addOption(Option.builder().longOpt("awsRegion").hasArg().desc("AWS Region").build());
        options.addOption(Option.builder().longOpt("awsS3Bucket").hasArg().desc("Resource where CSV's will be uploaded").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cli;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        // Run Source Connector
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(cli.getOptionValue("dataSourceUrl"));
        dataSource.setUser(cli.getOptionValue("user"));
        dataSource.setPassword(cli.getOptionValue("password"));
        final String schema = cli.getOptionValue("schemaName");
        try {
            for (String tableName : cli.getOptionValue("tableNames").split(",")) {
                SourceConnector sourceConnector = new PgSourceConnector(dataSource);
                SourceConnectionResult result = sourceConnector.write(schema, tableName);
                // Upload to s3
                AWSFileWriter awsFileWriter = new AWSFileWriter(
                        new AWSCredentials(cli.getOptionValue("awsAccessKeyId"), cli.getOptionValue("awsSecretKey")),
                        cli.getOptionValue("awsS3Bucket"),
                        cli.getOptionValue("awsRegion")
                );
                awsFileWriter.write(result.getCsvTableDataTypeDictionary(), "%s_dict.csv".formatted(tableName));
                awsFileWriter.write(result.getCsvTableContent(), "%s.csv".formatted(tableName));
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runSinkConnectorExample(String[] args) {
        var options = new Options();

        // Parse CLI Options
        options.addOption(Option.builder().longOpt("dataSourceUrl").hasArg().desc("data source URL for the sink connector").build());
        options.addOption(Option.builder().longOpt("schemaName").hasArg().required().desc("schema that will be copied").build());
        options.addOption(Option.builder().longOpt("tableNames").hasArg().required().desc("comma separated list of tables that will be exported to AWS").build());
        options.addOption(Option.builder().longOpt("user").hasArg().desc("").build());
        options.addOption(Option.builder().longOpt("password").hasArg().desc("").build());

        options.addOption(Option.builder().longOpt("awsAccessKeyId").hasArg().desc("AWS Access Key ID").build());
        options.addOption(Option.builder().longOpt("awsSecretKey").hasArg().desc("AWS Secret Key").build());
        options.addOption(Option.builder().longOpt("awsRegion").hasArg().desc("AWS Region").build());
        options.addOption(Option.builder().longOpt("awsS3Bucket").hasArg().desc("Resource where CSV's will be uploaded").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cli;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        // Run Source Connector
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(cli.getOptionValue("dataSourceUrl"));
        dataSource.setUser(cli.getOptionValue("user"));
        dataSource.setPassword(cli.getOptionValue("password"));
        final String schema = cli.getOptionValue("schemaName");
        try {
            for (String tableName : cli.getOptionValue("tableNames").split(",")) {
                SinkConnector sinkConnector = new PgSinkConnector(dataSource);
                // Get File From S3

                // Upload to s3
                AWSFileReader awsFileWriter = new AWSFileReader(
                        new AWSCredentials(cli.getOptionValue("awsAccessKeyId"), cli.getOptionValue("awsSecretKey")),
                        cli.getOptionValue("awsS3Bucket"),
                        cli.getOptionValue("awsRegion")
                );
                File csvfile = awsFileWriter.read("%s.csv".formatted(tableName));
                sinkConnector.write(schema, tableName, csvfile);
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}