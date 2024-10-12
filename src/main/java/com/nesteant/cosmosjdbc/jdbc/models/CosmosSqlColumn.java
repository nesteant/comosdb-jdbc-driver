package com.nesteant.cosmosjdbc.jdbc.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CosmosSqlColumn {

    public int type;
    public String name;

    public CosmosSqlColumn(String name, int type) {
        this.name = name;
        this.type = type;
    }
}
