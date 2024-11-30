package br.com.vilmasoftware.connector;

import oracle.jdbc.pool.OracleDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

class DataSourceFactory {
    public static PGSimpleDataSource createPostgresDataSource(String dataSourceUrl, String user, String password) {
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
}
