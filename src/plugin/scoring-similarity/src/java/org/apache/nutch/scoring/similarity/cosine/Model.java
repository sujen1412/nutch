package org.apache.nutch.scoring.similarity.cosine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil.StemFilterType;
import org.apache.nutch.scoring.similarity.util.LuceneTokenizer;
import org.apache.nutch.scoring.similarity.util.LuceneTokenizer.TokenizerType;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates a model used to store Document vector representation of the corpus. 
 * @author Sujen Shah
 *
 */
public class Model {

  public static ArrayList<DocVector> docVectors = new ArrayList<>();
  private static final Logger LOG = LoggerFactory.getLogger(Model.class);
  public static boolean isModelCreated = false;

  public static synchronized void createModel(Configuration conf) throws IOException {
    if(isModelCreated) {
      LOG.info("Model exists, skipping model creation");
      return;
    }
    LOG.info("Creating Cosine model");
    StringBuilder sb = new StringBuilder();
    try {
      String line;
      BufferedReader br = new BufferedReader(conf.getConfResourceAsReader((conf.get("cosine.goldstandard.file"))));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      LOG.info("Parsed content : {}", sb.length());
      docVectors.add(createDocVector(sb.toString()));
    } catch (Exception e) {
      LOG.warn("Failed to parse {} : {}",conf.get("cosine.goldstandard.file","goldstandard.txt.template"), 
          StringUtils.stringifyException(e));
    }
    if(docVectors.size()>0) {
      LOG.info("Cosine model creation complete");
      isModelCreated = true;
    }
    else
      LOG.info("Cosine model creation failed");
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
      DocVector docVector = new DocVector();
      docVector.setTermFreqVector(termVector);
      return docVector;
    } catch (IOException e) {
      LOG.error("Error creating DocVector for parsed page : {}",StringUtils.stringifyException(e));
    }
    return null;
  }

  public static float computeCosineSimilarity(DocVector docVector) {
    float scores[] = new float[docVectors.size()];
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

