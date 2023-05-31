package org.kafkatocas;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json2kafka.utils.JsonRecordSplitterPTransform;
import org.json2kafka.utils.ReferenceDataLoaderFromFileAsSideInput;
import org.json2kafka.utils.UserDataEnricherWithReferencePTransform;
import org.json2kafka.utils.UserLocationDataTable;
import org.kafkatocas.utils.KafkaOptions;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class KafkaToCassandra {

    public static void main(String[] args) throws NoSuchSchemaException {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        // Read from Kafka
        PCollection<String> data = pipeline.apply(
                KafkaIO.<String, String>read()
                        .withTopic(options.getTopic())
                        .withBootstrapServers(options.getBootstrapServers())
                        .withValueDeserializer(StringDeserializer.class)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withoutMetadata()

        ).apply(Values.<String>create());

        //Get reference map
        PCollectionView<Map<String, Map<String, String>>> referenceDataMap =
                pipeline.apply(new ReferenceDataLoaderFromFileAsSideInput(1L));

        PCollection<Map<String, String>> userLocationData = data.apply(new JsonRecordSplitterPTransform());

        //enrich the user location data with reference
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
