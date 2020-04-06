# One-to-many KTable - KTable

Run:

```bash
$ docker-compose up 
```

Then, you can start the Kafka Streams sample by running:

```bash

$ ./mvnw clean compile exec:java
[...]Waiting 2s before sending an update on the one topic
     [one]: 1, {"guid":1,"name":"one"}
     [many]: 1A, {"guid":1,"idem_id":"A","name":"manyOne"}
     [many]: 1B, {"guid":1,"idem_id":"B","name":"manyTwo"}
     [joined]: 1A, {"guid":1,"idem_id":"A","name":"manyOne","oneName":"one"}
     [joined]: 1B, {"guid":1,"idem_id":"B","name":"manyTwo","oneName":"one"}
     [joined]: 1A, {"guid":1,"idem_id":"A","name":"manyOne","oneName":"one"}
     [joined]: 1B, {"guid":1,"idem_id":"B","name":"manyTwo","oneName":"one"}
     2020-04-06 19:06:00.275 - INFO --- [       Thread-2] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
     [one]: 1, {"guid":1,"name":"un"}
     [joined]: 1A, {"guid":1,"idem_id":"A","name":"manyOne","oneName":"un"}
     [joined]: 1B, {"guid":1,"idem_id":"B","name":"manyTwo","oneName":"un"}
``` 

To interrupt this program, just press ctrl+c.
