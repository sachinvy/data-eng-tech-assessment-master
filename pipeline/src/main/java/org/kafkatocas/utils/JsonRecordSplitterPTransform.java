package org.json2kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class JsonRecordSplitterPTransform extends PTransform<PCollection<String>, PCollection<Map<String, String>>> {

    private static final String EVENT_ID_COLUMN_NAME = "event_id";
    private static final TupleTag<String> errorRecords = new TupleTag<>("ERROR_RECORDS_AT_SOURCE");

    @Override
    public PCollection<Map<String, String>> expand(PCollection<String> pCollection) {
        return pCollection.apply("Record Splitter", ParDo.of(new JsonRecordSplitterDoFn()))
                .setCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    }

    private static class JsonRecordSplitterDoFn extends DoFn<String, Map<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            try {
                List<Map<String, String>> userRecords = MultiLineJsonConvertor
                        .getMapStringFromString(context.element());

                for (Map<String, String> userRecord : userRecords) {
                    // Process each record as needed
//                    userRecord.put(EVENT_ID_COLUMN_NAME, UUID.randomUUID().toString());
                    context.output(userRecord);
                    System.out.println(userRecord);
                }
            } catch (IOException e) {
                log.error("Failed to process user records");
//                context.output(errorRecords, context.element());
            }
        }
    }

}
