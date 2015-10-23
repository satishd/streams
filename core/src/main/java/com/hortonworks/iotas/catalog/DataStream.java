package com.hortonworks.iotas.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageManager;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Data stream representing an IoTaS topology that will be persisted in
 * storage layer. Generated by UI
 */
public class DataStream implements Storable {

    public static final String DATA_STREAM_ID = "dataStreamId";
    public static final String DATA_STREAM_NAME= "dataStreamName";
    public static final String JSON = "json";
    public static final String TIMESTAMP = "timestamp";

    @Override
    public String toString () {
        return "DataStream{" +
                "dataStreamId=" + dataStreamId +
                ", dataStreamName='" + dataStreamName + '\'' +
                ", json='" + json + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataStream that = (DataStream) o;

        if (dataStreamId != null ? !dataStreamId.equals(that.dataStreamId) : that.dataStreamId != null)
            return false;
        if (dataStreamName != null ? !dataStreamName.equals(that.dataStreamName) : that.dataStreamName != null)
            return false;
        if (json != null ? !json.equals(that.json) : that.json != null)
            return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }

    @Override
    public int hashCode () {
        int result = dataStreamId != null ? dataStreamId.hashCode() : 0;
        result = 31 * result + (dataStreamName != null ? dataStreamName.hashCode() : 0);
        result = 31 * result + (json != null ? json.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }


    /**
     * Unique id identifying a data stream. This is the primary key column.
     */
    private Long dataStreamId;

    /**
     * Human readable data stream name; input from user from UI.
     */
    private String dataStreamName;

    /**
     * Json string representing the data stream; generated by UI.
     */
    private String json;

    /**
     * Time at which this data stream was created/updated.
     */
    private Long timestamp;


    public Long getDataStreamId () {
        return dataStreamId;
    }

    public void setDataStreamId (Long dataStreamId) {
        this.dataStreamId = dataStreamId;
    }

    public String getDataStreamName () {
        return dataStreamName;
    }

    public void setDataStreamName (String dataStreamName) {
        this.dataStreamName = dataStreamName;
    }

    public String getJson () {
        return json;
    }

    public void setJson (String json) {
        this.json = json;
    }

    public Long getTimestamp () {
        return timestamp;
    }

    public void setTimestamp (Long timestamp) {
        this.timestamp = timestamp;
    }

    @JsonIgnore
    public String getNameSpace () {return "datastreams";}

    @JsonIgnore
    public Schema getSchema () {
        return new Schema.SchemaBuilder().fields(
                new Schema.Field(DATA_STREAM_ID, Schema.Type.LONG),
                new Schema.Field(DATA_STREAM_NAME, Schema.Type.STRING),
                new Schema.Field(JSON, Schema.Type.STRING),
                new Schema.Field(TIMESTAMP, Schema.Type.LONG)
        ).build();

    }

    @JsonIgnore
    public PrimaryKey getPrimaryKey () {
        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<Schema.Field, Object>();
        fieldToObjectMap.put(new Schema.Field(DATA_STREAM_ID, Schema.Type.LONG),
                this.dataStreamId);
        return new PrimaryKey(fieldToObjectMap);
    }

    @JsonIgnore
    public StorableKey getStorableKey () {
        return new StorableKey(getNameSpace(), getPrimaryKey());
    }

    public Map toMap () {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(DATA_STREAM_ID, this.dataStreamId);
        map.put(DATA_STREAM_NAME, this.dataStreamName);
        map.put(JSON, this.json);
        map.put(TIMESTAMP, this.timestamp);
        return map;
    }

    public DataStream fromMap (Map<String, Object> map) {
        this.dataStreamId = (Long) map.get(DATA_STREAM_ID);
        this.dataStreamName = (String) map.get(DATA_STREAM_NAME);
        this.json = (String)  map.get(JSON);
        this.timestamp = (Long) map.get(TIMESTAMP);
        return this;
    }

}
