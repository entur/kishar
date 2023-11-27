package org.entur.kishar.gtfsrt.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

public class CompositeKey implements Serializable {

    private String id;

    private String datasource;

    private static ObjectMapper objectMapper = new ObjectMapper();

    public CompositeKey(String id, String datasource) {
        this.id = id;
        this.datasource = datasource;
    }

    private CompositeKey() {
        //Needed for Jackson unmarshalling
    }

    public static CompositeKey reCreate(byte[] input) {
        return create(new String(input));
    }

    public static CompositeKey create(String input) {
        try {
            return objectMapper.readValue(input, CompositeKey.class);
        }
        catch (IOException e) {
            // Ignore
        }
        return null;
    }

    public String asString() {
        try {
            return objectMapper.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    @Override
    public String toString() {
        return "CompositeKey{" +
                "id='" + id + '\'' +
                ", datasource='" + datasource + '\'' +
                '}';
    }
}
