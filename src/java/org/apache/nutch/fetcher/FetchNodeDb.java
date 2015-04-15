package org.apache.nutch.fetcher;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class FetchNodeDb {

  private Map<String, FetchNode> fetchNodeDbMap;
  private static FetchNodeDb fetchNodeDbInstance = null;
  
  public FetchNodeDb(){    
    System.out.println("Calling FetchNode constructor");
    fetchNodeDbMap = new ConcurrentHashMap<String, FetchNode>();
  }
  
  public static FetchNodeDb getInstance(){
    
    if(fetchNodeDbInstance == null){
      System.out.println("Creating FetchNode instance");
      fetchNodeDbInstance = new FetchNodeDb();
    }
    return fetchNodeDbInstance;
  }
  
  public void put(String url, FetchNode fetchNode){
    System.out.println("FetchNodeDb : putting node - " + fetchNode.hashCode());
    fetchNodeDbMap.put(url, fetchNode);    
  }  
  public Iterator<?> getFetchNodeDb(){
    return fetchNodeDbMap.entrySet().iterator();
  }
}


