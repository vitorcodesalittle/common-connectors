package br.com.vilmasoftware;

import br.com.vilmasoftware.connector.*;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import br.com.vilmasoftware.readers.AWSFileReader;
import br.com.vilmasoftware.writers.AWSFileWriter;

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        runSourceConnectorExample(args);
    }

    private static void runSourceConnectorExample(String[] args) {
        CommandLine cli = getCli(args);
        // Run Source Connector

        try {
            SourceConnector sourceConnector = createSourceConnector(cli.getOptionValue("dataSourceUrl"), cli.getOptionValue("user"), cli.getOptionValue("password"));
            final String schema = cli.getOptionValue("schemaName");
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
        CommandLine cli = getCli(args);

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

    private static CommandLine getCli(String[] args) {
        var options = new Options();
        setAwsOptions(options);
        setDataSourceOptions(options);
        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static SinkConnector createSinkConnector(String dataSourceUrl, String user, String password) throws NotImplementedException, SQLException {
        return SinkConnector.builder()
                .dataSourceUrl(dataSourceUrl)
                .user(user)
                .password(password)
                .build();
    }
    private static SourceConnector createSourceConnector(String dataSourceUrl, String user, String password) throws NotImplementedException, SQLException {
        return SourceConnector.builder()
                .dataSourceUrl(dataSourceUrl)
                .user(user)
                .password(password)
                .build();
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
