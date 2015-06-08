package org.apache.nutch.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.service.CrawlManager;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.service.model.request.JobConfig;
import org.apache.nutch.util.NutchTool;

public class CrawlManagerImpl implements CrawlManager{

  private final String INJECT = "org.apache.nutch.crawl.Injector";
  private final String GENERATE = "org.apache.nutch.crawl.Generator";
  private final String FETCH = "org.apache.nutch.fetcher.Fetcher";
  private final String UPDATE = "org.apache.nutch.crawl.CrawlDb";
  private final String INVERTLINKS = "org.apache.nutch.crawl.LinkDb";
  private final String INDEX = "org.apache.nutch.indexer.IndexingJob";
  private final String DEDUP = "org.apache.nutch.crawl.DeduplicationJob";
      
  JobFactory jobFactory;
  NutchTool tool;
  
  public CrawlManagerImpl(JobFactory jobFactory) {
    this.jobFactory = jobFactory;
  }
  
  public void create(JobConfig config) {
    String crawlId = config.getCrawlId();
    String confId = config.getConfId();
    Map<String, String> args  = config.getArgs();
    start(crawlId, args, confId); 
  }
  
  @Override
  public void start(String crawlId, Map<String, String> args, String confId) {
    Configuration conf = NutchServer.getInstance().getConfManager().get(confId);    
    int rounds = Integer.parseInt(args.get("rounds"));
    
    boolean index = false;
    if(args.containsKey("solrUrl"))
      index = true;
    
    try {
      tool = jobFactory.createToolByClassName(INJECT, conf);
      tool.run(args, crawlId);
      for(int i=0;i<rounds;i++){
        tool = jobFactory.createToolByClassName(GENERATE, conf);
        Map<String, Object> temp = tool.run(args, crawlId);
        Thread.sleep(100);
        
        tool = jobFactory.createToolByClassName(FETCH, conf);
        tool.run(args, crawlId);
        
        tool = jobFactory.createToolByClassName(UPDATE, conf);
        tool.run(args, crawlId);
        
        tool = jobFactory.createToolByClassName(INVERTLINKS, conf);
        tool.run(args, crawlId);
        
        tool = jobFactory.createToolByClassName(DEDUP, conf);
        tool.run(args, crawlId);
        
        if(index){
          tool = jobFactory.createToolByClassName(INDEX, conf);
          tool.run(args, crawlId);
        }
        
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
    
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }
}
