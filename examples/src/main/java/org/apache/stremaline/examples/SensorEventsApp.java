/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stremaline.examples;

import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iot.sensors.LocationRecord;
import org.apache.iot.sensors.SensorEvent;
import org.apache.iot.sensors.SensorType;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SensorEventsApp {
    private static final Logger LOG = LoggerFactory.getLogger(SensorEventsApp.class);
    private static final String TOPIC = "topicName";

    private final String registryURL;
    private final String producerPropsLoc;
    private final String schemaFileLoc;

    public SensorEventsApp(String producerPropsLoc, String registryURL, String schemaFileLoc) {
        this.producerPropsLoc = producerPropsLoc;
        this.registryURL = registryURL;
        this.schemaFileLoc = schemaFileLoc;
    }

    public void sendMessages() throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream(producerPropsLoc));

        int noOfRecords = Integer.parseInt(props.getProperty("maxAvroRecords", "50"));
        List<SensorEvent> avroRecords = generateSensorEventRecords(noOfRecords);

        // send avro messages to given topic using KafkaAvroSerializer which registers payload schema if it does not exist
        // with schema name as "<topic-name>:v", type as "avro" and schemaGroup as "kafka".
        // schema registry should be running so that KafkaAvroSerializer can register the schema.

        String topicName = props.getProperty(TOPIC);
        produceMessage(topicName, avroRecords, props);
    }

    private List<SensorEvent> generateSensorEventRecords(int noOfRecords) {
        int ct = noOfRecords;
        List<SensorEvent> records = new ArrayList<>(noOfRecords);
        while (--ct > 0) {
            SensorEvent record = SensorEvent.newBuilder()
                    .setXid(new Random().nextLong())
                    .setName("Sensor-" + ct)
                    .setTimestamp(System.nanoTime())
                    .setType(SensorType.values()[new Random().nextInt(2)])
                    .setVersion(1)
                    .setLocation(LocationRecord.newBuilder()
                                         .setLatitude(new Random().nextDouble() % 100)
                                         .setLongitude(new Random().nextDouble() % 100)
                                         .build())
                    .build();
            records.add(record);
        }

        return records;
    }

    private Map<String, Object> createProducerConfig(Properties props) {
        String bootstrapServers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), this.registryURL));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        return config;
    }


    private void produceMessage(String topicName, List<SensorEvent> msgs, Properties producerConfig) {
        LOG.info("No of messages [{}] to be sent to topic [{}] ", msgs.size(), topicName);
        final Producer<String, Object> producer = new KafkaProducer<>(producerConfig);
        final Callback callback = new MyProducerCallback();
        for (Object msg : msgs) {
            LOG.info("Sending message: [{}] to topic: [{}]", msg, topicName);
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, msg);
            producer.send(producerRecord, callback);
        }
        producer.flush();

        LOG.info("Message successfully sent to topic: [{}]", topicName);
        producer.close(5, TimeUnit.SECONDS);
    }

    private static class MyProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
            LOG.info("#### received [{}], ex: [{}]", recordMetadata, ex);
        }
    }

    /**
     * Print the command line options help message and exit application.
     */
    @SuppressWarnings("static-access")
    private static void showHelpMessage(String[] args, Options options) {
        Options helpOptions = new Options();
        helpOptions.addOption(Option.builder("h").longOpt("help")
                                      .desc("print this message").build());
        try {
            CommandLine helpLine = new DefaultParser().parse(helpOptions, args, true);
            if (helpLine.hasOption("help") || args.length == 1) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("truck-events-kafka-ingest", options);
                System.exit(0);
            }
        } catch (ParseException ex) {
            LOG.error("Parsing failed.  Reason: " + ex.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        Option producerFileOption = Option.builder("p").longOpt("producer-config").hasArg().desc("Provide a Kafka producer config file").type(String.class).build();
        Option registryUrlOption = Option.builder("r").longOpt("registry-url").hasArg().desc("Provide the registry URL").type(String.class).build();
        Option schemaOption = Option.builder("s").longOpt("schema-file").hasArg().desc("Provide a schema file").type(String.class).build();

        Options options = new Options();
        options.addOption(producerFileOption);
        options.addOption(registryUrlOption);
        options.addOption(schemaOption);
        showHelpMessage(args, options);

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine commandLine = parser.parse(options, args, false);

            Class<SensorEventsApp> clazz = SensorEventsApp.class;
            SensorEventsApp sensorEventsApp = new SensorEventsApp(commandLine.getOptionValue("p", clazz.getResource("/kafka-producer.props").getFile()),
                                                                  commandLine.getOptionValue("r", "http://localhost:9090/api/v1/schemaregistry"),
                                                                  commandLine.getOptionValue("s", clazz.getResource("/sensor-event.avsc").getFile()));
            sensorEventsApp.sendMessages();
        } catch (Exception e) {
            LOG.error("Failed to send messages ", e);
        }

    }
}
