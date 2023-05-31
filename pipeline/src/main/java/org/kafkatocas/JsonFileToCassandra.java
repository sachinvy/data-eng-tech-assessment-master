package org.kafkatocas;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json2kafka.utils.*;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.json2kafka.utils.JsonRecordSplitterPTransform;
import org.json2kafka.utils.ReferenceDataLoaderFromFileAsSideInput;
import org.json2kafka.utils.UserDataEnricherWithReferencePTransform;
import org.json2kafka.utils.UserLocationDataTable;
import org.kafkatocas.utils.KafkaOptions;

import java.util.*;

@Slf4j
public class JsonFileToCassandra {

    public static void main(String[] args) {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> data = pipeline.apply(TextIO.read().from(options.getFileFullPath()));


        PCollectionView<Map<String, Map<String, String>>> referenceDataMap =
                pipeline.apply(new ReferenceDataLoaderFromFileAsSideInput(1L));

        PCollection<Map<String, String>> userLocationData = data.apply(new JsonRecordSplitterPTransform());

        PCollection<Map<String, String>> userLocationWithReference = userLocationData.apply(
                new UserDataEnricherWithReferencePTransform(referenceDataMap)
        );

        userLocationWithReference.apply(ParDo.of(
                        new DoFn<Map<String, String>, UserLocationDataTable>() {

                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                context.output(new UserLocationDataTable(Objects.requireNonNull(context.element())));
                            }

                        }
                ))
                .apply(CassandraIO.<UserLocationDataTable>write()
                        .withHosts(Arrays.asList(options.getCassandraHost()))
                        .withPort(options.getCassandraPort())
                        .withEntity(UserLocationDataTable.class)
                        .withKeyspace("raw")

                )

        ;

        pipeline.run().waitUntilFinish();
    }

}
