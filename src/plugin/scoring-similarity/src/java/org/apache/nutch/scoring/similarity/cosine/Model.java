package org.apache.nutch.scoring.similarity.cosine;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil.StemFilterType;
import org.apache.nutch.scoring.similarity.util.LuceneIndexManager;
import org.apache.nutch.scoring.similarity.util.LuceneTokenizer;
import org.apache.nutch.scoring.similarity.util.LuceneTokenizer.TokenizerType;
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
  public static boolean isModelCreated = false;
  
  public static synchronized void createModel(String filesToIndex, Configuration conf) {
    if(isModelCreated) {
      LOG.info("Model exists, skipping model creation");
      return;
    }
    LOG.info("Creating Cosine model");
    LuceneIndexManager.createIndex(filesToIndex, new LuceneAnalyzerUtil(StemFilterType.PORTERSTEM_FILTER, true), conf);
    IndexReader reader = LuceneIndexManager.getIndexReader();
    if(reader != null) {
      populateTerms(reader);
      int numOfDocs = reader.numDocs();
      docVectors = new DocVector[numOfDocs];
      for(int docId=0; docId<numOfDocs; docId++) {
        docVectors[docId] = createDocVector(reader, docId);
      }
    }
    LOG.info("Cosine model creation complete");
    isModelCreated = true;
  }
  
  private static synchronized DocVector createDocVector(IndexReader reader, int docID) {
    DocVector docVector = new DocVector(allTerms.size());
    try {
      Terms terms = reader.getTermVector(docID, LuceneIndexManager.FIELD_CONTENT);
      TermsEnum itr = terms.iterator(null);
      BytesRef text;
      while((text = itr.next()) != null) {
        long freq = itr.totalTermFreq();
        docVector.setVectorEntry(allTerms.get(text.utf8ToString()), freq);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Error creating DocVector : {}",StringUtils.stringifyException(e));
    }
    return docVector;
  }

  private static synchronized void populateTerms(IndexReader reader) {
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
      LOG.error("Error populating terms in Cosine Model : {}",StringUtils.stringifyException(e));
    }
  }
  
  /**
   * Used to create a DocVector from given String text. Used during the parse stage of the crawl 
   * cycle to create a DocVector of the currently parsed page from the parseText attribute value
   * @param content
   */
  public static DocVector createDocVector(String content) {
      LuceneTokenizer tokenizer = new LuceneTokenizer(content, TokenizerType.CLASSIC, true, 
          StemFilterType.PORTERSTEM_FILTER);
      TokenStream tStream = tokenizer.getTokenStream();
      HashMap<String, Integer> termVector = new HashMap<>();
      try {
        CharTermAttribute charTermAttribute = tStream.addAttribute(CharTermAttribute.class);
        tStream.reset();
        while(tStream.incrementToken()) {
          String term = charTermAttribute.toString();
          if(termVector.containsKey(term)) {
            int count = termVector.get(term);
            count++;
            termVector.put(term, count);
          }
          else {
            termVector.put(term, 1);
          }
        }
        DocVector docVector = new DocVector(allTerms.size());
        for(Map.Entry<String, Integer> entry : termVector.entrySet()) {
          if(allTerms.containsKey(entry.getKey())) {
            docVector.setVectorEntry(allTerms.get(entry.getKey()), entry.getValue());
          }
        }
        return docVector;
      } catch (IOException e) {
        // TODO Auto-generated catch block
        LOG.error("Error creating docVector for parsed page : {}",StringUtils.stringifyException(e));
      }
      return null;
  }
  
  public static float computeCosineSimilarity(DocVector docVector) {
    float scores[] = new float[docVectors.length];
    int i=0;
    for(DocVector corpusDoc : docVectors) {
      float numerator = docVector.dotProduct(corpusDoc);
      float denominator = docVector.getL2Norm()*corpusDoc.getL2Norm();
      scores[i++] = numerator/denominator;
    }
    Arrays.sort(scores);
    System.out.println(Arrays.toString(scores));
    return scores[scores.length-1];
  }
}
