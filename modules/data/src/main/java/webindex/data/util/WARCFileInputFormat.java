/*
 * Copyright 2015 Webindex authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package webindex.data.util;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.archive.io.ArchiveReader;

/**
 * Minimal implementation of FileInputFormat for WARC files. Hadoop is told that splitting these
 * compressed files is not possible.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCFileInputFormat extends FileInputFormat<Text, ArchiveReader> {

  @Override
  public RecordReader<Text, ArchiveReader> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new WARCFileRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // As these are compressed files, they cannot be (sanely) split
    return false;
  }
}
