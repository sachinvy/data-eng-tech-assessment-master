package org.kafkatocas;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json2kafka.utils.JsonRecordSplitterPTransform;
import org.json2kafka.utils.ReferenceDataLoaderFromFileAsSideInput;
import org.json2kafka.utils.UserDataEnricherWithReferencePTransform;
import org.json2kafka.utils.UserLocationDataTable;
import org.kafkatocas.utils.KafkaOptions;
import org.kafkatocas.utils.MapToKVConvertorDoFn;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class JsonFileToKafka {

    public static void main(String[] args) {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> data = pipeline.apply(TextIO.read().from(options.getFileFullPath()));

        ImmutableMap<String, Object> producerProperties = ImmutableMap.<String, Object>builder()
                .put("request.timeout.ms", 12)
                .put("batch-size", "10")
                .put("ack", "all")
                .build();

        PCollection<Map<String, String>> userLocationData = data.apply(new JsonRecordSplitterPTransform());

        // convert records to KV pair for kafka
        userLocationData.apply(ParDo.of(new MapToKVConvertorDoFn()))
        .apply(KafkaIO.<String, String>write()
                .withTopic(options.getTopic())
                .withBootstrapServers(options.getBootstrapServers())
                .withProducerConfigUpdates(producerProperties)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
        )

        ;

        pipeline.run().waitUntilFinish();
    }

}
