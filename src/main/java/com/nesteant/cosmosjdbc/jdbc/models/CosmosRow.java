package com.nesteant.cosmosjdbc.jdbc.models;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.util.*;

@Getter
@Setter
public class CosmosRow {

    private List<Pair<String, Object>> parsed;
    private List<String> columns;
    private Map<Integer, Pair<String, Object>> index = new HashMap<>();

    public void setColumns(List<String> columns) {
        this.columns = columns;

        for (int i = 0; i < columns.size(); i++) {
            for (Pair<String, Object> pair : parsed) {
                if (pair.getLeft().equals(columns.get(i))) {
                    index.put(i, pair);
                }
            }
        }
    }

    public CosmosRow(Object object) {
        this.parsed = new ArrayList<>();

        if (object instanceof LinkedHashMap) {
            LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) object;
            for (String key : map.keySet()) {
                this.parsed.add(new ImmutablePair<>(key, map.get(key)));
            }
        }

        Field[] fields = object.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            try {
                this.parsed.add(new ImmutablePair<>(field.getName(), field.get(object)));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    public CosmosRow(LinkedHashMap<String, Object> map) {

        this.parsed = new ArrayList<>();

        for (String key : map.keySet()) {
            this.parsed.add(new ImmutablePair<>(key, map.get(key)));
        }
    }

    @Override
    public String toString() {
        return "CosmosRow{" +
                "parsed=" + parsed +
                '}';
    }
}
