package org.apache.nutch.service;

import java.util.Map;

import org.apache.nutch.service.model.request.JobConfig;

public interface CrawlManager {
    
  public void stop();

  public void create(JobConfig config);

  void start(String crawlId, Map<String, String> args, String confId);
  
}
