package io.confluent;

import java.util.List;

import static java.util.Arrays.asList;

public interface Topics {
    String ONE_TOPIC = "one";
    String MANY_TOPIC = "many";
    String JOINED_TOPIC = "joined";

    List<String> all = asList(ONE_TOPIC, MANY_TOPIC, JOINED_TOPIC);
}
