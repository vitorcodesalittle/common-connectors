package br.com.vilmasoftware.connector;

import br.com.vilmasoftware.DataSourceSupportedProviders;
import br.com.vilmasoftware.connector.exceptions.NotImplementedException;
import br.com.vilmasoftware.connector.impl.SourceConnectorImpl;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;

public class SourceConnectorBuilder {
    private String dataSourceUrl;
    private String user;
    private String password;
    private String awsBucketName;
    private String awsRegionName;

    public SourceConnectorBuilder dataSourceUrl(String value) {
        this.dataSourceUrl = value;
        return this;
    }

    public SourceConnectorBuilder user(String value) {
        user = value;
        return this;
    }

    public SourceConnectorBuilder password(String value) {
        password = value;
        return this;
    }

    public SourceConnectorBuilder awsRegionName(String value) {
        awsRegionName = value;
        return this;
    }

    public SourceConnectorBuilder awsBucket(String value) {
        awsBucketName = value;
        return this;
    }

    public SourceConnector build() throws NotImplementedException {
        if (dataSourceUrl == null || dataSourceUrl.isEmpty()) {
            throw new IllegalArgumentException("Datasource URL cannot be null or empty");
        }

        try {
            var uri = new URI(dataSourceUrl);
            String scheme = uri.getSchemeSpecificPart();
            if (scheme != null) {
                String[] parts = scheme.split(":");
                if (parts.length > 0) {
                    var notImplemented =  new NotImplementedException("%s is not a supported datasource".formatted(parts[0]));
                    switch (DataSourceSupportedProviders.byJdbcId(parts[0])
                            .orElseThrow(() -> notImplemented)) {
                        case POSTGRES -> {
                            var dataSource = DataSourceFactory.createPostgresDataSource(dataSourceUrl, user, password);
                            return new SourceConnectorImpl(dataSource, awsBucketName, awsRegionName);
                        }
                        case ORACLE -> {
                            var dataSource = DataSourceFactory.createOracleDataSource(dataSourceUrl, user, password);
                            return new SourceConnectorImpl(dataSource, awsBucketName, awsRegionName);
                        }
                    }

                }
            }
            throw new IllegalArgumentException("Invalid datasource URL format");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid datasource URL", e);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Creating datasource failed with SQLException", e);
        }
    }
}
