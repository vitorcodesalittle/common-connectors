package br.com.vilmasoftware;

import br.com.vilmasoftware.connector.*;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import lombok.SneakyThrows;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
	public static void main(String[] args) {
		try {
			CommandLine cli = getCli(args);
			if (cli.getOptionValue("type").equals("source")) {
				runSourceConnectorExample(cli);
			} else if (cli.getOptionValue("type").equals("sink")) {
				runSinkConnectorExample(cli);
			} else {
				System.err.println("invalid options. must have 'sink' or 'source'");
				System.exit(1);
			}
		} catch (Exception e) {
			System.err.println("unknown exception thrown during execution");
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}

	@SneakyThrows
	private static void runSourceConnectorExample(CommandLine cli) {
		SourceConnector sourceConnector = SourceConnector.builder().dataSourceUrl(cli.getOptionValue("dataSourceUrl"))
				.user(cli.getOptionValue("user")).password(cli.getOptionValue("password"))
				.awsBucket(cli.getOptionValue("awsBucket")).awsRegionName(cli.getOptionValue("awsRegion")).build();
		try (var sourceStream = SourceRequest.fromFileAsStream(cli.getOptionValue("etl-file"))) {
			sourceConnector.write(sourceStream);
		}
	}

	private static void runSinkConnectorExample(CommandLine cli) {
		try {
			// Run Source Connector
			SinkConnector sinkConnector = SinkConnector.builder().dataSourceUrl(cli.getOptionValue("dataSourceUrl"))
					.user(cli.getOptionValue("user")).password(cli.getOptionValue("password")).build();
			final String schema = cli.getOptionValue("schemaName");
			for (String tableName : cli.getOptionValue("tableNames").split(",")) {
				sinkConnector.write(schema, tableName);
			}
		} catch (IOException | SQLException | NotImplementedException e) {
			throw new RuntimeException(e);
		}
	}

	private static CommandLine getCli(String[] args) {
		var options = new Options();
		options.addOption(Option.builder().longOpt("type").desc("Can be 'sink' or 'source'").build());
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
		options.addOption(Option.builder().longOpt("dataSourceUrl").hasArg()
				.desc("data source URL for the source connector").build());
		options.addOption(
				Option.builder().longOpt("schemaName").hasArg().required().desc("schema that will be copied").build());
		options.addOption(Option.builder().longOpt("etl-file").hasArg().required()
				.desc("A file with a json array that describes the schema, table and queries.").build());
		options.addOption(Option.builder().longOpt("user").hasArg().desc("").build());
		options.addOption(Option.builder().longOpt("password").hasArg().desc("").build());
	}

	private static void setAwsOptions(Options options) {
		options.addOption(Option.builder().longOpt("awsRegion").hasArg().desc("AWS Region").build());
		options.addOption(
				Option.builder().longOpt("awsS3Bucket").hasArg().desc("Resource where CSV's will be uploaded").build());
	}
}
