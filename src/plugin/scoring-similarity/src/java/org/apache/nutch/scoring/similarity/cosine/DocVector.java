package org.apache.nutch.scoring.similarity.cosine;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

public class DocVector {

  public RealVector vector;
  
  public DocVector(String content){
    
  }
  
  public DocVector(int size) {
    vector = new ArrayRealVector(size);
  }
  
  public void setVectorEntry(int pos, long freq) {
      vector.addToEntry(pos, (double)freq);
  }
  
}
