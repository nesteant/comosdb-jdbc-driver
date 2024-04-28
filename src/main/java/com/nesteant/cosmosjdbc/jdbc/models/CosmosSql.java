package com.nesteant.cosmosjdbc.jdbc.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CosmosSql {

    private String sql;

    private String container;

    private String database;

    public CosmosSql(String sql, String container, String database) {
        this.sql = sql;
        this.container = container;
        this.database = database;
    }
}
