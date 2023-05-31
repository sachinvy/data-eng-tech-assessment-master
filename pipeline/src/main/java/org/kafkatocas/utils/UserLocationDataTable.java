package org.json2kafka.utils;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

@Table(keyspace = "raw", name = "user_location_table", readConsistency = "ONE", writeConsistency = "ONE",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
public class UserLocationDataTable implements Serializable {
    private static final long serialVersionUID = 1L;

//    @PartitionKey
//    @Column(name = "event_id")
//    public String event_id;

    @PartitionKey
    @Column(name = "timestamp")
    public String timestamp;

    @Column(name = "location_id")
    public String locationid;

    @Column(name = "direction_1")
    public String direction1;

    @Column(name = "direction_2")
    public String direction2;

    @Column(name = "total_of_directions")
    public String totalOfDirections;

    @Column(name = "sensor_description")
    public String sensor_description;

    @Column(name = "sensor_description")
    public String sensor_name;

    @Column(name = "installation_date")
    public String installation_date;


    @Column(name = "location_type")
    public String location_type;
    @Column(name = "status")
    public String status;

    @Column(name = "direction_1_desc")
    public String direction_1_desc;

    @Column(name = "direction_2_desc")
    public String direction_2_desc;

    @Column(name = "latitude")
    public String latitude;

    @Column(name = "longitude")
    public String longitude;


    public UserLocationDataTable() {
    }

    public UserLocationDataTable(Map<String, String> userLocationData) {
        this.locationid = userLocationData.get("locationid");
//        this.event_id = userLocationData.get("event_id");
        this.status = userLocationData.get("status");
        this.direction1 = userLocationData.get("direction_1");
        this.direction2 = userLocationData.get("direction_2");
        this.direction_1_desc = userLocationData.get("direction_1_desc");
        this.direction_2_desc = userLocationData.get("direction_2_desc");
        this.installation_date = userLocationData.get("installation_date");
        this.timestamp = userLocationData.get("timestamp");
        this.latitude = userLocationData.get("latitude");
        this.longitude = userLocationData.get("longitude");
        this.location_type = userLocationData.get("location_type");
        this.sensor_description = userLocationData.get("sensor_description");
        this.sensor_name = userLocationData.get("sensor_name");
        this.totalOfDirections = userLocationData.get("total_of_directions");

    }

    @Override
    public boolean equals(Object obj) {
        UserLocationDataTable other = (UserLocationDataTable) obj;
        return this.locationid.equals(other.locationid) && this.timestamp.equals(other.timestamp);
    }
}