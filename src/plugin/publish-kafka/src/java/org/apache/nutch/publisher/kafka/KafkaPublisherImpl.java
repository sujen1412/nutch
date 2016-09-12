/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.publisher.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.nutch.fetcher.FetcherThreadEvent;
import org.apache.nutch.publisher.NutchPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaPublisherImpl implements NutchPublisher{

  private static String BOOTSTRAP_SERVER;
  private static String TOPIC;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisherImpl.class);
  private static Properties props;
  private static Producer<String, String> producer;

  @Override
  public boolean setConfig(Configuration conf) {

    try {
      if(producer == null) {
        Map<String, String> params = conf.getValByRegex("kafka.*");
        
        BOOTSTRAP_SERVER = params.remove("kafka.bootstrap.servers");
        TOPIC = params.remove("kafka.topic");

        if (TOPIC == null) {
          throw new Exception("Kafka topic name not provided");
        }
        if (BOOTSTRAP_SERVER == null) {
          throw new Exception("Kafka bootstrap servers not provided");
        }
        
        props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        /** Load user provided properties */
        for(Map.Entry<String, String> pair: params.entrySet()){
          LOG.debug("Setting Kafka prop - {}: {}", pair.getKey(), pair.getValue());
          props.put(pair.getKey().replace("kafka.", ""), pair.getValue());
        }
        producer = new KafkaProducer<String, String>(props);
      }
    }catch(Exception e){
      LOG.warn("Could not configure Kafka publisher: {}",e.getMessage());
      return false;
    }

    return true;
  }

  @Override
  public void publish(Object event, Configuration conf) {
    producer.send(new ProducerRecord<String, String>(TOPIC, getJSONString(event)));
  }

  private String getJSONString(Object obj) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      LOG.error("Error converting event object to JSON String - {}", StringUtils.stringifyException(e));
    }
    return null;
  }

  @Override
  public Configuration getConf() {	
    return null;
  }

  @Override
  public void setConf(Configuration arg0) {

  }
}
