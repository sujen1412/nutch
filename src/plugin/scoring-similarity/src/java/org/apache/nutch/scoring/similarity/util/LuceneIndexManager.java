/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.scoring.similarity.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the functionality to read/write Lucene indexes
 * @author Sujen Shah
 *
 */
public class LuceneIndexManager {
 
  private static Directory ramDirectory = new RAMDirectory();
  public static final String FIELD_CONTENT = "contents";
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexManager.class);
  private static boolean isIndexed = false;

  public static synchronized void createIndex(String fileToIndexDirectory, Analyzer analyzer, Configuration conf) {
    if(isIndexed) {
      return;
    }
    
    try {
      LOG.info("Setting files to index directory to {}", fileToIndexDirectory);
      IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(Version.LATEST, analyzer));
      Path dir = new Path(fileToIndexDirectory);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fileStatus = fs.listStatus(dir);
      Document document = new Document();
      FieldType type = new FieldType();
      type.setIndexed(true);
      type.setStoreTermVectors(true);
      Field field = null;
      BufferedReader br;
      for(int i=0;i<fileStatus.length;i++) {
        br = new BufferedReader(new InputStreamReader(fs.open(fileStatus[i].getPath())));
        
        if(field == null) {
          field = new Field(FIELD_CONTENT, br, type);
        }
        else {
          field.setReaderValue(br);
        }
        LOG.debug("Adding {} file to index",fileStatus[i].getPath());
        document.add(field);
        indexWriter.addDocument(document);
        document.removeField(FIELD_CONTENT);
      }
      indexWriter.close();
      isIndexed = true;
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public static IndexReader getIndexReader() {
    try {
      IndexReader reader = DirectoryReader.open(ramDirectory);
      return reader;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Could not open reader : ", StringUtils.stringifyException(e));
      e.printStackTrace();
    }
    return null;
  }
}
