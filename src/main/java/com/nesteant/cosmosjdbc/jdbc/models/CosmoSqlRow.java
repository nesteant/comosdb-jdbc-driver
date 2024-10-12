package com.nesteant.cosmosjdbc.jdbc.models;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class CosmoSqlRow {

    private List<CosmosSqlColumn> columns = new ArrayList<>();
    private List<CosmosSqlCell> cells = new ArrayList<>();

    public CosmoSqlRow(Object object) {

    }


    private List<CosmosSqlColumn> getColumns(Object object) {
        return null;
    }
}
