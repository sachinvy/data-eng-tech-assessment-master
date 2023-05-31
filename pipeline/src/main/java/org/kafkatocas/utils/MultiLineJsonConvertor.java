package org.json2kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.data.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiLineJsonConvertor {

    private static final String LOCATION_ID_REFERENCE = "location_id";


    private static final ImmutableMap<String, String> REFERENCE_COLUMN_MAP = ImmutableMap.<String, String>builder()
            .put("sensor_description", "sensor_description")
            .put("sensor_name", "sensor_name")
            .put("installation_date", "installation_date")
            .put("location_type", "location_type")
            .put("status", "status")
            .put("direction_1", "direction_1_desc")
            .put("direction_2", "direction_2_desc")
            .put("latitude", "latitude")
            .put("longitude", "longitude")
            .build();


    public static List<Map<String, Object>> getMapObjectFromString(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(jsonString,
                new TypeReference<List<Map<String, Object>>>() {
                });
    }

    public static List<Map<String, String>> getMapStringFromString(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(jsonString,
                new TypeReference<List<Map<String, String>>>() {
                });
    }

    public static List<String> getJsonObjectAsStringFromString(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> outputStringList = new ArrayList<>();
        List<Map<String, String>> objectMap = objectMapper.readValue(jsonString,
                new TypeReference<List<Map<String, String>>>() {
                });

        for (Map<String, String> element : objectMap
        ) {
            outputStringList.add(objectMapper.writeValueAsString(element));
        }

        return outputStringList;


    }


    public static Map<String, Map<String, String>> convertJsonToMapBasedOnLocationId(List<Map<String, Object>> locationJson) {
        Map<String, Map<String, String>> jsonMapBasedOnLocationId = new HashMap<>();

        for (Map<String, Object> mapEntry : locationJson
        ) {
            Map<String, String> locationMap = new HashMap<>();

            for (Map.Entry<String, Object> entry : mapEntry.entrySet()
            ) {
                if (REFERENCE_COLUMN_MAP.containsKey(entry.getKey()) && entry.getValue() != null) {
                    locationMap.put(REFERENCE_COLUMN_MAP.get(entry.getKey()), entry.getValue().toString());
                }

            }

            jsonMapBasedOnLocationId.put(mapEntry.get(LOCATION_ID_REFERENCE).toString(), locationMap);
        }
        System.out.println(jsonMapBasedOnLocationId);
        return jsonMapBasedOnLocationId;
    }

}
