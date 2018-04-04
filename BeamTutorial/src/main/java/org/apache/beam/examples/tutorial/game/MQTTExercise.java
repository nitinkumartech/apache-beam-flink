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
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MQTTExercise {

  /**
   * A transform to extract key/score information from SensorData, and sum
   * the scores. The constructor arg determines whether 'team' or 'user' info is
   * extracted.
   */
  public static class ExtractAndSumScore
      extends PTransform<PCollection<SensorData>, PCollection<KV<String, Integer>>> {

    private final KeyField field;

    ExtractAndSumScore(KeyField field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<SensorData> gameInfo) {
      return gameInfo
        .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */)
        .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */);
    }
  }

  /**
   * Run a batch pipeline.
   */
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        // Generate a bounded set of data.
        .apply(MqttIO.read()
                .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
                        "tcp://139.162.46.222:1883",
                        "sample_topic")));
        // Extract and sum username/score pairs from the event data.
        //.apply("ExtractUserScore", new ExtractAndSumScore(KeyField.DEVICEID))
        // Write the user and score to the "user_score" BigQuery table.
        //.apply(new Output.WriteUserScoreSums(options.getOutputPrefix()));

    // Run the batch pipeline.
    pipeline.run();
  }
}
