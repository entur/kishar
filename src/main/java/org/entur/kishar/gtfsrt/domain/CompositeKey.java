package org.entur.kishar.gtfsrt.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;

public class CompositeKey implements Serializable {

    private String id;

    private String datasource;

    private static transient ObjectMapper objectMapper = new ObjectMapper();

    public CompositeKey(String id, String datasource) {
        this.id = id;
        this.datasource = datasource;
    }

    public CompositeKey() {

    }

    public static CompositeKey reCreate(byte[] input) {
        CompositeKey recreatedKey = CompositeKey.create(new String(input));
        if (recreatedKey != null) {
            return recreatedKey;
        }

        final AlertKey alertKey = AlertKey.create(input);
        if (alertKey != null) {
            recreatedKey.datasource = alertKey.getDatasource();
            recreatedKey.id = alertKey.getId();
            return recreatedKey;
        }
        final TripUpdateKey tripUpdateKey = TripUpdateKey.create(input);
        if (tripUpdateKey != null) {
            recreatedKey.datasource = tripUpdateKey.getDatasource();
            recreatedKey.id = tripUpdateKey.getId();
            return recreatedKey;
        }
        final VehiclePositionKey vehiclePositionKey = VehiclePositionKey.create(input);
        if (vehiclePositionKey != null) {
            recreatedKey.datasource = vehiclePositionKey.getDatasource();
            recreatedKey.id = vehiclePositionKey.getId();
            return recreatedKey;
        }

        return null;
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
