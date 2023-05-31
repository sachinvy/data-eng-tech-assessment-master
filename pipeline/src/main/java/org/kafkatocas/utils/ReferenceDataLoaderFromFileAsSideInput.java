package org.json2kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.json2kafka.JsonFileToCassandra;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@Slf4j
public class ReferenceDataLoaderFromFileAsSideInput extends PTransform<PBegin, PCollectionView<Map<String, Map<String, String>>>> {
    private long reloadFrequencyInHours;

    public ReferenceDataLoaderFromFileAsSideInput(long reloadFrequencyInHours) {
        this.reloadFrequencyInHours = reloadFrequencyInHours;
    }

    @Override
    public PCollectionView<Map<String, Map<String, String>>> expand(PBegin pBegin) {
        return pBegin.apply("Loading reference data as side input",
                        GenerateSequence.from(0).withRate(1,
                                Duration.standardHours(this.reloadFrequencyInHours)))
                .apply(
                        ParDo.of(new ReferenceDataLoaderFromFile()
                        )).setCoder(MapCoder.of(StringUtf8Coder.of(), MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
                .apply(
                        Window.<Map<String, Map<String, String>>>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                                .discardingFiredPanes())
                .apply(Latest.globally())
                .apply(View.asSingleton());
    }

    public static class ReferenceDataLoaderFromFile extends DoFn<Long, Map<String, Map<String, String>>> {
        @ProcessElement
        public void process(OutputReceiver<Map<String, Map<String, String>>> o, ProcessContext context) {
            try {
                String referenceString = Files.readString(Path.of(context.getPipelineOptions().as(JsonFileToCassandra.KafkaOptions.class).getReferenceFilePath()));

                o.output(MultiLineJsonConvertor
                        .convertJsonToMapBasedOnLocationId(
                                MultiLineJsonConvertor.getMapObjectFromString(referenceString)));
            } catch (IOException e) {
                log.error("Failed to Parse input reference file");
                throw new TechnicalException(TechnicalErrorCodes.FAILED_TO_LOAD_REFERENCE_FILE.toString(), e);
            }
        }
    }

}
