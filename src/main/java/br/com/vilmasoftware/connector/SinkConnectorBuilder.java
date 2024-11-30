package br.com.vilmasoftware.connector;

import br.com.vilmasoftware.DataSourceSupportedProviders;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import br.com.vilmasoftware.connector.impl.PgSinkConnector;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;
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
    public SinkConnectorBuilder awsRegion(String value) {
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
                            dataSource = DataSourceFactory.createPostgresDataSource(dataSourceUrl, user, password);
                            return new PgSinkConnector(dataSource, awsBucket, awsRegion);
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
}
