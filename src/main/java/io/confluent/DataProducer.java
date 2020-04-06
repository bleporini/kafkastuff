package io.confluent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.confluent.Exceptions.uncheckIt;
import static io.confluent.Topics.MANY_TOPIC;
import static io.confluent.Topics.ONE_TOPIC;

public class DataProducer {

    private final Properties configuration;

    public DataProducer(Properties configuration) {
        this.configuration = configuration;
    }

    public void uncheckedProduceData(){
        new Thread(this::uncheckedProduceDataSync).start();
    }

    public void uncheckedProduceDataSync(){
        uncheckIt(this::produceDataSync);
    }


    private void produceDataSync() throws ExecutionException, InterruptedException {

        String one = "{\"guid\":1, \"name\": \"one\"}";
        String oneUpdated = "{\"guid\":1, \"name\": \"un\"}";
        String many1A = "{\"guid\":1, \"idem_id\":\"A\", \"name\": \"manyOne\"}";
        String many1B = "{\"guid\":1, \"idem_id\":\"B\", \"name\": \"manyTwo\"}";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configuration)) {

            producer.send(
                    new ProducerRecord<>(
                            ONE_TOPIC,
                            "1",
                            one
                    )
            ).get();
            producer.send(
                    new ProducerRecord<>(
                            MANY_TOPIC,
                            "1A",
                            many1A
                    )
            ).get();
            producer.send(
                    new ProducerRecord<>(
                            MANY_TOPIC,
                            "1B",
                            many1B
                    )
            ).get();

            System.out.println("Waiting 2s before sending an update on the " + ONE_TOPIC + " topic");
            Thread.sleep(2000);

            producer.send(
                    new ProducerRecord<>(
                            ONE_TOPIC,
                            "1",
                            oneUpdated
                    )
            ).get();

        }
    }

}
