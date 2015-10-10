package org.apache.nutch.scoring.similarity.cosine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil.StemFilterType;
import org.apache.nutch.scoring.similarity.util.LuceneIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates a model used to store Document vector representation of the corpus. 
 * @author Sujen Shah
 *
 */
public class Model {

  public static DocVector[] docVectors;
  private static Map<String, Integer> allTerms;
  private static final Logger LOG = LoggerFactory.getLogger(Model.class);
  
  public void createModel(String filesToIndex, Configuration conf) {
    LOG.info("Creating model");
    LuceneIndexManager.createIndex(filesToIndex, new LuceneAnalyzerUtil(StemFilterType.PORTERSTEM_FILTER, true), conf);
    IndexReader reader = LuceneIndexManager.getIndexReader();
    if(reader != null) {
      populateTerms(reader);
      int numOfDocs = reader.numDocs();
      for(int docId=0; docId<=numOfDocs; docId++) {
        docVectors[docId] = createDocVector(reader, docId);
      }
    }
    LOG.debug("Model creation complete");
  } 
  
  private DocVector createDocVector(IndexReader reader, int docID) {
    DocVector docVector = new DocVector(allTerms.size());
    try {
      Terms terms = reader.getTermVector(docID, LuceneIndexManager.FIELD_CONTENT);
      TermsEnum itr = terms.iterator(null);
      BytesRef text;
      while((text = itr.next()) != null) {
        long freq = itr.totalTermFreq();
        docVector.setVectorEntry(allTerms.get(text), freq);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return docVector;
  }

  private void populateTerms(IndexReader reader) {
    allTerms = new HashMap<String, Integer>();
    try {
      Terms terms = MultiFields.getTerms(reader, LuceneIndexManager.FIELD_CONTENT);
      TermsEnum itr = terms.iterator(null);
      BytesRef text;
      int pos = 0;
      while((text = itr.next()) != null) {
        String term = text.utf8ToString();
        allTerms.put(term, pos++);       
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
