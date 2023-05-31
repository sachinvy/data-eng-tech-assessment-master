package org.json2kafka.utils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JsonRecordSplitterPTransformTest {

    static final String INPUT_JSON = "[{\"id\" : 1, \"value\" : \"a\"}, {\"id\" : 2, \"value\" : \"b\"}]";

    @Test
    public void TestExpected() {
        Map<String, String> expected_element1 = new HashMap<>();
        expected_element1.put("id", "1");
        expected_element1.put("value", "a");

        Map<String, String> expected_element2 = new HashMap<>();
        expected_element2.put("id", "2");
        expected_element2.put("value", "b");

        List<Map<String, String>> expectedOutput = Arrays.asList(expected_element1, expected_element2);

        Pipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

        PCollection<Map<String, String>> output = p.apply(Create.of(INPUT_JSON))
                .apply(new JsonRecordSplitterPTransform());
        PAssert.that(output).containsInAnyOrder(expectedOutput);
        p.run();

    }

}