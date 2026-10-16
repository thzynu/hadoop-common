package org.apache.hadoop.examples;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordMedian extends Configured implements Tool {

  private double median = 0;
  private final static IntWritable ONE = new IntWritable(1);

  /**
   * Maps words from line of text into a key-value pair; the length of the word
   * as the key, and 1 as the value.
   */
  public static class WordMedianMapper extends
      Mapper<Object, Text, IntWritable, IntWritable> {

    private IntWritable length = new IntWritable();

    /**
     * Emits a key-value pair for counting the word. Outputs are (IntWritable,
     * IntWritable).
     * 
     * @param value
     *          This will be a line of text coming in from our input file.
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String string = itr.nextToken();
        length.set(string.length());
        context.write(length, ONE);
      }
    }
  }

  /**
   * Performs integer summation of all the values for each key.
   */
  public static class WordMedianReducer extends
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable val = new IntWritable();

    /**
     * Sums all the individual values within the iterator and writes them to the
     * same key.
     * 
     * @param key
     *          This will be a length of a word that was read.
     * @param values
     *          This will be an iterator of all the values associated with that
     *          key.
     */
    public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      val.set(sum);
      context.write(key, val);
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new WordMedian(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: wordmedian <in> <out>");
      return 0;
    }

    setConf(new Configuration());
    Configuration conf = getConf();

    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "word median");
    job.setJarByClass(WordMedian.class);
    job.setMapperClass(WordMedianMapper.class);
    job.setCombinerClass(WordMedianReducer.class);
    job.setReducerClass(WordMedianReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean result = job.waitForCompletion(true);

    // Wait for JOB 1 -- get middle value to check for Median

    long totalWords = job.getCounters()
        .getGroup(TaskCounter.class.getCanonicalName())
        .findCounter("MAP_OUTPUT_RECORDS", "Map output records").getValue();
    int medianIndex1 = (int) Math.ceil((totalWords / 2.0));
    int medianIndex2 = (int) Math.floor((totalWords / 2.0));

    median = MedianFinder.readAndFindMedian(args[1], medianIndex1, medianIndex2, conf);

    return (result ? 0 : 1);
  }

  public double getMedian() {
    return median;
  }
}
