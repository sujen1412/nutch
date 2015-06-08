package org.apache.nutch.service.model.response;

import java.util.Map;


public class CrawlInfo {

  public static enum State {
    RUNNING, FINISHED, FAILED, KILLED, STOPPING, KILLING, PAUSED
  };
  
  private String id;
  private String confId;
  private Map<String, String> args;
  private Map<String, Object> result;
  
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getConfId() {
    return confId;
  }
  public void setConfId(String confId) {
    this.confId = confId;
  }
  public Map<String, String> getArgs() {
    return args;
  }
  public void setArgs(Map<String, String> args) {
    this.args = args;
  }
  public Map<String, Object> getResult() {
    return result;
  }
  public void setResult(Map<String, Object> result) {
    this.result = result;
  }
  public State getState() {
    return state;
  }
  public void setState(State state) {
    this.state = state;
  }
  public String getMsg() {
    return msg;
  }
  public void setMsg(String msg) {
    this.msg = msg;
  }
  public String getCrawlId() {
    return crawlId;
  }
  public void setCrawlId(String crawlId) {
    this.crawlId = crawlId;
  }
  private State state;
  private String msg;
  private String crawlId;
}
