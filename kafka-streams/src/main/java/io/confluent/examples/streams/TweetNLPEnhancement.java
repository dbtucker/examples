/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.examples.streams;

/**
 * Demonstrates enriching an incoming data stream and publishing the enriched records.
 * <p>
 * In this example we ingest Twitter events produced by a Kafka Connect Source and enhance
 * them with some sentiment analysis of the tweet itself.
 *
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, Confluent Schema Registry, and Kafka Connect worker.
 * You'll need to include the Twitter Source Connector in your Connect worker configuration.
 * Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic tweets \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic tweets.enriched \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic tweets.sentiment \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 * <p>
 * 3) Start the Twitter Source connector so that events arrive on the 'tweets' topic.
 * <p>
 * 4) Start this example application either in your IDE or on the command line.
 * <p>
 *
 */

/* Internal details :

    Twitter stream enhanced with NLP-based sentiment analysis of tweet,
    as well as re-structuring GeoLocation fields to be compatible with Couch.

    Designed to handle either flavor of Connect Converter (JSON or Avro),
    with or without schema.

    Data Flow
        inbound topic: tweets (overridable with tweets.input.topic property)
        outbound topics  (overridable with tweets.enriched.topic and tweets.sentiment.topic):
            tweets.enriched     # with NLP sentiment
            tweets.sentiment    # sentiments scores and tweet id only

    We tried one approach where users had to specify the the converter type
    (and thus the topology).   The second approach was to default to AVRO and
    then catch any exception thrown by the topology and fall back to JSON.

 */

import io.confluent.examples.streams.utils.NLP;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.examples.streams.utils.GenericAvroSerde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.lang.System.exit;

public class TweetNLPEnhancement {

    static public class SentimentValue {
        public String sentiment;
        public Integer sentimentScore ;
    }

    private static final Map<Integer, String> scores;

    static {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "Very Negative");
        map.put(1, "Negative");
        map.put(2, "Neutral");
        map.put(3, "Positive");
        map.put(4, "Very Positive");
        scores = Collections.unmodifiableMap(map);
    }

    private static String TWEETS_INPUT_CONFIG="tweets.input.topic";
    private static String TWEETS_ENRICHED_CONFIG="tweets.enriched.topic";
    private static String TWEETS_SENTIMENT_CONFIG="tweets.sentiment.topic";
    private static String APP_DEFAULT_SERDES="app.default.serdes";

    private static Schema enrichedAvroSchema;
    private static Map<String, Object> enrichedJsonSchema;
    private static Boolean redeployTopology;

    private static KafkaStreams streams;

    private static void topologyExceptionHandler (Thread t, Throwable e)
    {
        System.out.println("  Uncaught exception thrown in topology ");
        redeployTopology = true;
    }

    private static void deployJsonTopology (Properties streamsConfiguration)
            throws Exception
    {
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final String srcTopic=streamsConfiguration.get(TWEETS_INPUT_CONFIG).toString();
        final String enrichedTopic=streamsConfiguration.get(TWEETS_ENRICHED_CONFIG).toString();
        final String sentimentTopic=streamsConfiguration.get(TWEETS_SENTIMENT_CONFIG).toString();

        ObjectMapper mapper = new ObjectMapper();

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> tweets = builder.stream(srcTopic);
        final KStream<String, Map<String, Object>> deserialized = tweets
                .map((key, value) -> {
                    //System.out.println(key + " " + value);
                    Map<String, Object> jsonMap = null;
                    try {
                        jsonMap = mapper.readValue(value, new TypeReference<Map<String, Object>>() { });
                    } catch (IOException e) {
                    }

                   return new KeyValue<>(key, jsonMap);
               });

        final KStream<String, Map<String, Object>> enrichedTweets = deserialized
                .map((key, jsonMap) -> {
                    Map<String, Object> sourceRecord = (Map<String,Object>)jsonMap;
                    Map<String, Object> payloadRecord = sourceRecord;
                    Map<String, Object> schemaRecord = null;
                    Long id;
                    String sentiment;
                    int sentimentScore;
                    List<Double> newLoc = new ArrayList<>();

                        /* Incoming record may be with or without schema */
                    if (sourceRecord.containsKey("payload") && sourceRecord.containsKey("schema")) {
                        payloadRecord = (Map<String,Object>)sourceRecord.get("payload");
                        schemaRecord = (Map<String,Object>)sourceRecord.get("schema");
                    }

                        /* Update the schema with the fields we may add */
                        /* TBD : use kafka.connect.schema classes to do this work */
                        /* TBD : do this only once and cache the result */
                    if (schemaRecord != null  &&  enrichedJsonSchema == null) {
                        String schemaName = schemaRecord.get("name").toString() + "." + enrichedTopic;
                        schemaRecord.put("name", schemaName);

                        Map<String, Object> ssSchemaRecord = new HashMap<String, Object>();
                        ssSchemaRecord.put ("field", "SentimentScore".toString());
                        ssSchemaRecord.put ("type", "int32".toString());
                        ssSchemaRecord.put ("doc", "Numeric representation of message sentiment".toString());
                        ssSchemaRecord.put ("optional", (Boolean)true);

                        Map<String, Object> sSchemaRecord = new HashMap<String, Object>();
                        sSchemaRecord.put ("field", "Sentiment".toString());
                        sSchemaRecord.put ("type", "string".toString());
                        sSchemaRecord.put ("doc", "Assessed sentiment of tweet".toString());
                        sSchemaRecord.put ("optional", (Boolean)true);

                        Map<String, Object> lSchemaRecord = new HashMap<String, Object>();
                        Map<String, Object> lItems = new HashMap<String, Object>();
                        lItems.put ("type", "double".toString());
                        lItems.put ("optional", (Boolean)false);
                        lSchemaRecord.put ("field", "Location".toString());
                        lSchemaRecord.put ("type", "array".toString());
                        lSchemaRecord.put ("doc", "Couchbase-compatibile location field { array of lat/long}".toString());
                        lSchemaRecord.put ("optional", (Boolean)true);
                        lSchemaRecord.put ("items", lItems);

                        ArrayList<Map<String,Object>> schemaFields = (ArrayList<Map<String,Object>>) schemaRecord.get("fields");
                        schemaFields.add (ssSchemaRecord);
                        schemaFields.add (sSchemaRecord);
                        schemaFields.add (lSchemaRecord);

                        /*  TO BE DONE ... real deep copy
                        if (enrichedJsonSchema == null) {
                            enrichedJsonSchema = new HashMap<>();
                            for (Map.Entry<String, Object> entry : schemaRecord.entrySet()) {
                                enrichedJsonSchema.put(entry.getKey(), entry.getValue().clone());
                            }
                        }
                        */
                    }

                    id = (Long)payloadRecord.get("Id");
                    if(payloadRecord.containsKey("GeoLocation") && payloadRecord.get("GeoLocation") != null) {
                        Map<String,Object> loc = (Map<String,Object>)payloadRecord.get("GeoLocation");
                        Double lon = (Double)loc.getOrDefault("Longitude", 0);
                        Double lat = (Double)loc.getOrDefault("Latitude", 0);

                        newLoc.add(lon);
                        newLoc.add(lat);
                    }

                    if (!payloadRecord.containsKey("SentimentScore")) {
                        String text = (String)payloadRecord.get("Text");
                        sentimentScore = NLP.findSentiment(text);
                        sentiment = scores.get(sentimentScore);

                        payloadRecord.put("SentimentScore", sentimentScore);
                        payloadRecord.put("Sentiment", sentiment);
                        payloadRecord.put ("Location", newLoc);
                    } else {
                        sentimentScore = (int)payloadRecord.get("SentimentScore");
                        sentiment =  scores.get(sentimentScore);
                    }

                    System.out.println("Enriched tweet: " + id.toString() + " (" + sentiment + ")");

                    if (schemaRecord == null) {
                        sourceRecord = payloadRecord;
                    } else {
                        sourceRecord.put("payload", payloadRecord);
                        sourceRecord.put("schema", enrichedJsonSchema == null ? schemaRecord : enrichedJsonSchema);
                    }
                    return new KeyValue<>(key, sourceRecord);
                });

            /* Dump enriched tweets topic */
        enrichedTweets
                .map((key, jsonMap) -> {
                    String valString;
                    try {
                        valString = mapper.writeValueAsString(jsonMap);
                    }
                    catch (Exception e) {
                        valString = null;
                    }

                    return new KeyValue<>(key,valString);
                }).to(enrichedTopic);


            // TO DO ... Add Schema support to tweets.sentiment
        final KStream<String, String> sentiments = enrichedTweets
                .map((key, jsonMap) -> {
                    Map<String, Object> doc = (Map<String, Object>) jsonMap;

                    if (doc.containsKey("payload") && doc.containsKey("schema")) {
                        doc = (Map<String,Object>)doc.get("payload");
                    }
                    Long id = (Long)doc.get("Id");
                    Map<String, Object> retMap = new HashMap<String, Object>();

                    retMap.put ("sentiment", (String)doc.get("Sentiment"));
                    retMap.put ("sentimentScore", (Integer)doc.get("SentimentScore"));
                    String retString;
                    try {
                        retString = mapper.writeValueAsString(retMap);
                    }
                    catch (Exception e) {
                        retString = null;
                    }

                    return new KeyValue<>(id.toString(), retString );
                });

        sentiments.to(sentimentTopic);

        streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void deployAvroTopology (Properties streamsConfiguration)
            throws Exception
    {
        // streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        // streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        final String srcTopic=streamsConfiguration.get(TWEETS_INPUT_CONFIG).toString();
        final String enrichedTopic=streamsConfiguration.get(TWEETS_ENRICHED_CONFIG).toString();
        final String sentimentTopic=streamsConfiguration.get(TWEETS_SENTIMENT_CONFIG).toString();

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<GenericRecord, GenericRecord> tweets = builder.stream(srcTopic);
        final KStream<GenericRecord, GenericRecord> enrichedTweets = tweets
             .map((key, value) -> {
                 GenericRecord retVal = null;
                 Long id = (Long) value.get("Id");
                 String sentiment;
                 int sentimentScore;
                 List<Double> newLoc = new ArrayList<>();

                    // Make sure we have our schema to work with
                    // Assumes that all our input records will have same schema (safe enough assumption for now)
                 if (enrichedAvroSchema == null) {
                     String enrichedNamespace = value.getSchema().getNamespace();
                     enrichedAvroSchema = Schema.createRecord(value.getSchema().getName()+"."+enrichedTopic, "Default Twitter schema plus sentiment records", enrichedNamespace, false);

                        // The new fields will be optional (to match the architecture of the original schema)
                     Schema ssSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
                     Schema sSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
                     Schema lSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.createArray(Schema.create(Schema.Type.DOUBLE))));

                     List<Field> retFields = new ArrayList<>();
                     for (Field f : value.getSchema().getFields()) {
                         retFields.add(new Field(f.name(),f.schema(),f.doc(),f.defaultValue()));
                     }

                     // retFields.add(new Field("SentimentScore",Schema.create(Schema.Type.INT),"Numeric representation of message sentiment",null));
                     retFields.add(new Field("SentimentScore", ssSchema,"Numeric representation of message sentiment", null));
                     retFields.add(new Field("Sentiment", sSchema, "Assessed sentiment of tweet", null));
                     retFields.add(new Field("Location", lSchema, "Couchbase-compatibile location field { array of lat/long}", null));

                     enrichedAvroSchema.setFields(retFields);
                 }

                 if (value.get("SentimentScore") == null) {
                     String text = value.get ("Text").toString();
                     sentimentScore = NLP.findSentiment(text);
                     sentiment = scores.get(sentimentScore);

                     if (value.get("GeoLocation") != null) {
                         GenericRecord loc = (GenericRecord)value.get("GeoLocation");
                         Double lon = (Double)loc.get("Longitude");
                         Double lat = (Double)loc.get("Latitude");

                         if (lon != null  && lat != null) {
                             newLoc.add(lon);
                             newLoc.add(lat);
                         }
                     }

                     retVal = new GenericData.Record(enrichedAvroSchema);

                        // Option 1: copy everything
                     for (Field f : value.getSchema().getFields()) {
                         retVal.put(f.name(), value.get(f.name()));
                     }
                        // Option 2: copy key fields (+ ones with no default values)
                     // for (String f : Arrays.asList("CreatedAt", "Id", "Text", "User", "GeoLocation")) {
                     //     retVal.put(f, value.get(f));
                     // }
                     // for (String f : Arrays.asList("Contributors", "WithheldInCountries")) {
                     //     retVal.put(f, value.get(f));
                     // }


                     retVal.put("SentimentScore", Integer.valueOf(sentimentScore));
                     retVal.put("Sentiment", sentiment);
                     retVal.put("Location", newLoc);
                 } else {
                     sentimentScore = (int)value.get("SentimentScore");
                     sentiment =  scores.get(sentimentScore);

                     retVal = value;
                 }

                 System.out.println("Enriched tweet: " + id.toString() + " (" + sentiment + ")");
                 return new KeyValue<>(key,retVal);
             });

            /* Dump enriched tweets topic */
        enrichedTweets.to(enrichedTopic);

        streams = new KafkaStreams(builder, streamsConfiguration);
        streams.setUncaughtExceptionHandler(TweetNLPEnhancement::topologyExceptionHandler);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void main(final String[] args) throws Exception {
        enrichedAvroSchema = null;
        enrichedJsonSchema = null;
        redeployTopology = false;

        final Properties streamsConfiguration = new Properties();
        Properties progConfiguration = new Properties();

        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-sentiment");
        streamsConfiguration.put(TWEETS_INPUT_CONFIG, "tweets");
        streamsConfiguration.put(TWEETS_ENRICHED_CONFIG, "tweets.enriched");
        streamsConfiguration.put(TWEETS_SENTIMENT_CONFIG, "tweets.sentiment");

        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        // Specify default (de)serializers for record keys and for record values.

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        streamsConfiguration.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        // Override defaults with explicit input (if present)
        String[] pNames = {
                TWEETS_INPUT_CONFIG,
                TWEETS_ENRICHED_CONFIG,
                TWEETS_SENTIMENT_CONFIG,
                APP_DEFAULT_SERDES,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                StreamsConfig.KEY_SERDE_CLASS_CONFIG,
                StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
        };
        String pValue;
        if (args.length > 0) {
            System.out.println("Reading property overrides from " + args[0]);
            // Known properties that we can override
            InputStream propInputStream = null;

            try {
                propInputStream = new FileInputStream(args[0]);
                progConfiguration.load(propInputStream);

                for (String prop : pNames) {
                    pValue = progConfiguration.getProperty(prop);
                    if (pValue != null) {
                        System.out.println("  Overriding " + prop + " with " + pValue);
                        streamsConfiguration.put(prop, pValue);
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                if (propInputStream != null) {
                    propInputStream.close();
                }
            }
        }

        // Lastly, command-line overrides (highest precedence)
        System.out.println("Checking for command-line overrides");
        for (String prop : pNames) {
            pValue = System.getProperty(prop);
            if (pValue != null) {
                System.out.println("  Overriding " + prop + " with " + pValue);
                streamsConfiguration.put(prop, pValue);
            }
        }

        // Init Stanford NLP library
        NLP.init();
        // Warm up NLP processor
        NLP.findSentiment("hello");

        String defaultSerdes = streamsConfiguration.getProperty(APP_DEFAULT_SERDES, "avro");

        if (defaultSerdes.compareToIgnoreCase("avro") == 0) {
            deployAvroTopology(streamsConfiguration);
        } else {
            deployJsonTopology(streamsConfiguration);
        }

        // The Avro Topology will set the redeployTopology flag for any uncaught exception,
        // which is most likely a deserialization error.   On that assumption, we'll
        // fall back to the json topology.   This avoids the hassle of requiring the
        // APP_DEFAULT_SERDES parameter and getting it wrong.
        while (! redeployTopology) {
            if (streams.state() == KafkaStreams.State.PENDING_SHUTDOWN) break;
            Thread.sleep(250);
        }
        if (redeployTopology) {
            streams.close();
            deployJsonTopology(streamsConfiguration);
            System.in.read();
        }
    }
}
