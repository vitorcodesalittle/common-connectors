package br.com.vilmasoftware;

import lombok.Getter;

import java.util.Optional;

@Getter
public enum DataSourceSupportedProviders {
    POSTGRES("postgresql"), ORACLE("oracle");

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
