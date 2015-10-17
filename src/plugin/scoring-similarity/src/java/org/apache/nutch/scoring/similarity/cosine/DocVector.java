package org.apache.nutch.scoring.similarity.cosine;

import java.util.HashMap;
import java.util.Map;

public class DocVector {

  public HashMap<Integer, Long> termVector;

  public DocVector(int size) {
    termVector = new HashMap<>();
  }

  public void setVectorEntry(int pos, long freq) {
    termVector.put(pos, freq);
  }
  
  public float dotProduct(DocVector docVector) {
    float product = 0;
    for(Map.Entry<Integer, Long> entry : termVector.entrySet()) {
      if(docVector.termVector.containsKey(entry.getKey())) {
        product += docVector.termVector.get(entry.getKey())*entry.getValue();
      }
    }
    return product;
  }
  
  public float getL2Norm() {
    float sum = 0;
    for(Map.Entry<Integer, Long> entry : termVector.entrySet()) {
      sum += entry.getValue()*entry.getValue();
    }
    return (float) Math.sqrt(sum);
  }

}
