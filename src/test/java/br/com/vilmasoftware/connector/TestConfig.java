package br.com.vilmasoftware.connector;

import br.com.vilmasoftware.connector.impl.S3SimpleTableResolver;

public class TestConfig {
    public static String sourceDataSourceUrl = "jdbc:postgresql://localhost:5432/Adventureworks";
    public static String sinkDataSourceUrl = "jdbc:postgresql://localhost:5432/Adventureworks2";
    public static String user = "postgres";
    public static String password = "postgres";
    public static String awsBucket = "operai-1asd818d3818d3d18";
    public static String awsRegion = "us-east-1";
    public final static TableResolver s3SimpleTableResolver = new S3SimpleTableResolver("data");

}
