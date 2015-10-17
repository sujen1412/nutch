package org.apache.nutch.scoring.similarity.cosine;

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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
    // TODO Auto-generated method stub
    this.conf = conf;
  }

  @Override
  public float setURLScoreAfterParsing(Text url, Content content, Parse parse) {
    // TODO Auto-generated method stub
    if(!Model.isModelCreated){
      Model.createModel(conf.get("cosine.corpus.path"), conf);
    }
    String metatags = parse.getData().getParseMeta().get("metatag.keyword");
    String metaDescription = parse.getData().getParseMeta().get("metatag.description");
    DocVector docVector = Model.createDocVector(parse.getText()+metaDescription+metatags);
    
    float score = Model.computeCosineSimilarity(docVector);
    LOG.info("Setting score of {} to {}",url, score);
    return score;
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData,
      Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust,
      int allCount) {
    // TODO Auto-generated method stub
    return null;
  }

}
