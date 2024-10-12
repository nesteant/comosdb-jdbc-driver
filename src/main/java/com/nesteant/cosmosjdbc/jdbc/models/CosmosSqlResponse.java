package com.nesteant.cosmosjdbc.jdbc.models;

import com.azure.cosmos.util.CosmosPagedIterable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CosmosSqlResponse {

    private CosmosPagedIterable<Object> iterable;

    public static CosmosSqlResponse from(CosmosPagedIterable<Object> iterable) {
        CosmosSqlResponse response = new CosmosSqlResponse();
        response.setIterable(iterable);
        return response;
    }
}
