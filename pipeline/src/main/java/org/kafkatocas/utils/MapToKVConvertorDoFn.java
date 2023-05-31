package org.kafkatocas.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.Map;
import java.util.UUID;

@Slf4j
public class MapToKVConvertorDoFn extends DoFn<Map<String, String>, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            context.output(KV.of(UUID.randomUUID().toString(), objectMapper.writeValueAsString(context.element())));
        } catch (JsonProcessingException e) {
            log.error("Failed to write the records as string");
        }
    }
}

