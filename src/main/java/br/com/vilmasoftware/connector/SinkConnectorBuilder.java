package br.com.vilmasoftware.connector;

import br.com.vilmasoftware.DataSourceSupportedProviders;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import br.com.vilmasoftware.connector.impl.PgSinkConnector;

import javax.sql.DataSource;
import java.sql.SQLException;

public class SinkConnectorBuilder {

    private String dataSourceUrl;
    private String user;
    private String password;
    private String awsRegion;
    private String awsBucket;

    public SinkConnectorBuilder dataSourceUrl(String value) {
        this.dataSourceUrl = value;
        return this;
    }

    public SinkConnectorBuilder user(String value) {
        user = value;
        return this;
    }

    public SinkConnectorBuilder password(String value) {
        password = value;
        return this;
    }

    public SinkConnectorBuilder awsRegionName(String value) {
        awsRegion = value;
        return this;
    }

    public SinkConnectorBuilder awsBucket(String value) {
        awsBucket = value;
        return this;
    }

    public SinkConnector build() throws NotImplementedException, SQLException {
        if (dataSourceUrl == null || dataSourceUrl.isEmpty()) {
            throw new IllegalArgumentException("Datasource URL cannot be null or empty");
        }
        DataSource dataSource;
        var notImplemented = new NotImplementedException("%s is not a supported datasource url".formatted(dataSourceUrl));
        switch (DataSourceSupportedProviders.byJdbcId(dataSourceUrl)
                .orElseThrow(() -> notImplemented)) {
            case POSTGRES -> {
                dataSource = DataSourceFactory.createPostgresDataSource(dataSourceUrl, user, password);
                return new PgSinkConnector(dataSource, awsBucket, awsRegion);
            }
            case ORACLE -> {
                throw notImplemented;
            }
        }
        throw new IllegalArgumentException("Invalid datasource URL format: %s".formatted(dataSourceUrl));
    }
}
