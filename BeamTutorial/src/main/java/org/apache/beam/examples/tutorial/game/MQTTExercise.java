/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.examples.tutorial.game;

import org.apache.beam.examples.tutorial.game.SensorData.KeyField;
import org.apache.beam.examples.tutorial.game.utils.ChangeMe;
import org.apache.beam.examples.tutorial.game.utils.ExerciseOptions;
import org.apache.beam.examples.tutorial.game.utils.Input;
import org.apache.beam.examples.tutorial.game.utils.Output;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import javax.annotation.Nullable;

public class MQTTExercise {
    //org.apache.beam.examples.tutorial.game.MQTTExercise
    public static class ExtractWordsFn extends DoFn<byte[], String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            // Split the line into words.
            try {
                String json = new String(c.element(), "UTF-8");
                String[] words = json.split("[^a-zA-Z']+");

                // Output each word encountered into the output PCollection.
                for (String word : words) {
                    if (!word.isEmpty()) {
                        c.output(word);
                    }
                }
            } catch (Exception e) {

            }
        }
    }

    public static class CountWords extends PTransform<PCollection<byte[]>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<byte[]> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts =
                    words.apply(Count.perElement());

            return wordCounts;
        }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, byte[]> {
        @Override
        public byte[] apply(KV<String, Long> input) {
            return (input.getKey() + ": " + input.getValue()).getBytes();
        }
    }
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        // Generate a bounded set of data.
        .apply(MqttIO.read()
                .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
                        "tcp://139.162.46.222:1883",
                        "sample_topic")))
            .apply(Window.into(
                    FixedWindows.of(Duration.standardMinutes(1L))))
            .apply(new CountWords())
            .apply(MapElements.via(new FormatAsTextFn()))
            .apply(MqttIO.write().withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
                    "tcp://139.162.46.222:1883",
                    "output_topic")));
        // Extract and sum username/score pairs from the event data.
        //.apply("ExtractUserScore", new ExtractAndSumScore(KeyField.DEVICEID))
        // Write the user and score to the "user_score" BigQuery table.
        //.apply(new Output.WriteUserScoreSums(options.getOutputPrefix()));

    // Run the batch pipeline.
    pipeline.run();
  }
}
