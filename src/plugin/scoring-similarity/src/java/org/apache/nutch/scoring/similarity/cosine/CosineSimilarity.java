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
package org.apache.nutch.scoring.similarity.cosine;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.similarity.SimilarityModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosineSimilarity implements SimilarityModel{

  private Configuration conf; 
  private final static Logger LOG = LoggerFactory
      .getLogger(CosineSimilarity.class);

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public float setURLScoreAfterParsing(Text url, Content content, Parse parse) {
    float score = 1;

    try {
      if(!Model.isModelCreated){
        Model.createModel(conf);
      }
      String metatags = parse.getData().getParseMeta().get("metatag.keyword");
      String metaDescription = parse.getData().getParseMeta().get("metatag.description");
      DocVector docVector = Model.createDocVector(parse.getText()+metaDescription+metatags);
      score = Model.computeCosineSimilarity(docVector);
      LOG.info("Setting score of {} to {}",url, score);
    } catch (Exception e) {
      LOG.error("Error creating Cosine Model, setting scores of urls to 1 : {}", StringUtils.stringifyException(e));
    }
    return score;
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData,
      Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust,
      int allCount) {
    return null;
  }

}
