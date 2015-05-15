package org.apache.nutch.fetcher;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class FetchNodeDb {

  private Map<Integer, FetchNode> fetchNodeDbMap;
  private int index;
  private static FetchNodeDb fetchNodeDbInstance = null;
  
  public FetchNodeDb(){    
//    System.out.println("Calling FetchNode constructor");
    fetchNodeDbMap = new ConcurrentHashMap<Integer, FetchNode>();
    index = 1;
  }
  
  public static FetchNodeDb getInstance(){
    
    if(fetchNodeDbInstance == null){
//      System.out.println("Creating FetchNode instance");
      fetchNodeDbInstance = new FetchNodeDb();
    }
//    System.out.println("FetchNodeDb Instance : " + fetchNodeDbInstance);
    return fetchNodeDbInstance;
  }
  
  public void put(String url, FetchNode fetchNode){
    System.out.println("FetchNodeDb : putting node - " + fetchNode.hashCode());
    fetchNodeDbMap.put(index++, fetchNode);    
  }  
  public Map<Integer, FetchNode> getFetchNodeDb(){
    return fetchNodeDbMap;
  }
}


