package org.json2kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class UserDataEnricherWithReferencePTransform extends PTransform<PCollection<Map<String, String>>, PCollection<Map<String, String>>> {

    private static final String LOCATION_ID_COLUMN_NAME = "locationid";
    private static final TupleTag<Map<String, String>> errorRecords = new TupleTag<>("ERROR_RECORDS");
    private final PCollectionView<Map<String, Map<String, String>>> userReferenceSideInput;

    public UserDataEnricherWithReferencePTransform(PCollectionView<Map<String, Map<String, String>>> userReferenceSideInput) {
        this.userReferenceSideInput = userReferenceSideInput;
    }

    @Override
    public PCollection<Map<String, String>> expand(PCollection<Map<String, String>> userDataPCol) {
        return userDataPCol.apply(
                ParDo.of(new UserDataEnricherWithReferenceDoFn())
                        .withSideInput("RefMap", this.userReferenceSideInput)

        ).setCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    }

    private class UserDataEnricherWithReferenceDoFn extends DoFn<Map<String, String>, Map<String, String>> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            Map<String, String> userRecord = context.element();

            Map<String, Map<String, String>> refMap = context.sideInput(userReferenceSideInput);

            if (userRecord != null) {
                Map<String, String> enrichedUserRecords = new HashMap<>(userRecord);

                // Process each record as needed
                if (userRecord.get(LOCATION_ID_COLUMN_NAME) != null) {
                    Map<String, String> refDataForGivenLocation = refMap.get(userRecord.get(LOCATION_ID_COLUMN_NAME));
                    enrichedUserRecords.putAll(refDataForGivenLocation);
                    context.output(enrichedUserRecords);
                } else {
                    log.error("Location information in not available in the input records logging to error queue.");
//                    context.output(errorRecords, userRecord);
                }

            } else {
                log.error("failed to enrich the user location information");
            }
        }
    }
}
