package org.entur.kishar.gtfsrt.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

@SuppressWarnings("unused")
public class CompositeKey implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeKey.class);

    private String id;

    private String datasource;

    private static final ObjectMapper objectMapper = new ObjectMapper();

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
            LOG.warn("Failed to serialize CompositeKey", e);
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
