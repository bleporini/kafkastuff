package io.confluent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static io.confluent.Exceptions.uncheckIt;
import static io.confluent.Topics.JOINED_TOPIC;
import static io.confluent.Topics.MANY_TOPIC;
import static io.confluent.Topics.ONE_TOPIC;
import static java.lang.System.getProperty;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

public class KTableNonKeyJoinO2M {


    private final Properties configuration = configure();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new KTableNonKeyJoinO2M().run();

    }

    private void run() throws ExecutionException, InterruptedException {
        prepareSync();

        final CountDownLatch latch = new CountDownLatch(1);

        Topology topology = buildTopology().build();

        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, configuration);


        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    streams.close();
                    latch.countDown();
                })
        );

        try {
            streams.setStateListener((newState, oldState) -> {
                if (RUNNING.equals(newState))
                    new DataProducer(configuration).uncheckedProduceData();
            });
            streams.start();

            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }

    }


    private StreamsBuilder buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, JsonNode> oneTable = builder.table(
                ONE_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        new JsonSerde()
                )
        );


        KTable<String, JsonNode> manyTable = builder.table(
                MANY_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        new JsonSerde()
                )
        );

        KStream<String, ObjectNode> joined = manyTable.join(
                oneTable,
                many ->
                        many.get("guid").toString(),
                (many, one) -> {
                    ObjectNode many1 = (ObjectNode) many;
                    many1.put("oneName", one.get("name").textValue());
                    return many1;
                }
        ).toStream();
        joined.print(Printed.<String, ObjectNode>toSysOut().withLabel(JOINED_TOPIC));
        joined.to(JOINED_TOPIC);

        oneTable.toStream().print(Printed.<String, JsonNode>toSysOut().withLabel(ONE_TOPIC));
        manyTable.toStream().print(Printed.<String, JsonNode>toSysOut().withLabel(MANY_TOPIC));

        return builder;

    }

    /**
     * Drop topics and recreate it.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void prepareSync() throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(configuration);
        Supplier<List<String>> listTopicsToDelete = () ->
                uncheckIt(() ->
                        adminClient.listTopics()
                                .names()
                                .get()
                                .stream()
                                .filter(Topics.all::contains)
                                .collect(toList())
                );

        List<String> toDelete = listTopicsToDelete.get();

        adminClient.deleteTopics(toDelete).all().get();

        do {
            Thread.sleep(100);
            toDelete = listTopicsToDelete.get();
            System.out.println("toDelete = " + toDelete);
        }while (! toDelete.isEmpty());

        adminClient.createTopics(
                Topics.all
                        .stream()
                        .map(s -> new NewTopic(s, 1, (short)1))
                        .collect(toList())
        ).all().get();
    }


    private Properties configure(){
        Properties conf = new Properties();

        try (final var propFile = this.getClass().getResourceAsStream(getProperty("stuff.config"))) {
            conf.load(propFile);

            conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

            conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return conf;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
