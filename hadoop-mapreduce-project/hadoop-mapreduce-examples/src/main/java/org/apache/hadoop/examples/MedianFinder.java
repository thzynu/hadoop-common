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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

/**
 * Reads the output of the word-length count job and finds the median value.
 * Extracted from {@link WordMedian}.
 */
class MedianFinder {

  /**
   * This is a standard program to read and find a median value based on a file
   * of word counts such as: 1 456, 2 132, 3 56... Where the first values are
   * the word lengths and the following values are the number of times that
   * words of that length appear.
   * 
   * @param path
   *          The path to read the HDFS file from (part-r-00000...00001...etc).
   * @param medianIndex1
   *          The first length value to look for.
   * @param medianIndex2
   *          The second length value to look for (will be the same as the first
   *          if there are an even number of words total).
   * @throws IOException
   *           If file cannot be found, we throw an exception.
   * */
  static double readAndFindMedian(String path, int medianIndex1,
      int medianIndex2, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(path, "part-r-00000");

    if (!fs.exists(file))
      throw new IOException("Output not found!");

    BufferedReader br = null;

    try {
      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
      int num = 0;

      String line;
      while ((line = br.readLine()) != null) {
        StringTokenizer st = new StringTokenizer(line);

        // grab length
        String currLen = st.nextToken();

        // grab count
        String lengthFreq = st.nextToken();

        int prevNum = num;
        num += Integer.parseInt(lengthFreq);

        if (medianIndex2 >= prevNum && medianIndex1 <= num) {
          System.out.println("The median is: " + currLen);
          br.close();
          return Double.parseDouble(currLen);
        } else if (medianIndex2 >= prevNum && medianIndex1 < num) {
          String nextCurrLen = st.nextToken();
          double theMedian = (Integer.parseInt(currLen) + Integer
              .parseInt(nextCurrLen)) / 2.0;
          System.out.println("The median is: " + theMedian);
          br.close();
          return theMedian;
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    // error, no median found
    return -1;
  }
}
