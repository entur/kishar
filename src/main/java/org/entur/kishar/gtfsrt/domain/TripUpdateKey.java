package org.entur.kishar.gtfsrt.domain;

import java.io.*;

public class TripUpdateKey implements Serializable {

    private String id;

    private String datasource;

    public TripUpdateKey(String id, String datasource) {
        this.id = id;
        this.datasource = datasource;
    }

    public static TripUpdateKey create(byte[] input) {
        try(ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(input))) {
            return (TripUpdateKey)in.readObject();
        }
        catch (Exception e) {
            throw new IllegalStateException("could not create object", e);
        }
    }

    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            return bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
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
        return "TripUpdateKey{" +
                "id='" + id + '\'' +
                ", datasource='" + datasource + '\'' +
                '}';
    }
}
