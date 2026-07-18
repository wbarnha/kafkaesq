// Validates a kafkaesq-generated .properties file against the Apache Kafka
// Java client: every key must be a known ConsumerConfig name, and the file
// must construct a real KafkaConsumer (which validates values). The Java
// client only logs warnings for unknown keys, so the explicit configNames()
// check is what makes unknown keys fail. No broker is contacted.
//
// Usage: java ValidateConfig client.properties
import java.io.FileInputStream;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ValidateConfig {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(args[0])) {
            props.load(in);
        }

        Set<String> known = ConsumerConfig.configNames();
        boolean ok = true;
        for (String key : props.stringPropertyNames()) {
            if (!known.contains(key)) {
                System.err.println("unknown Java consumer config: " + key);
                ok = false;
            }
        }
        if (!ok) {
            System.exit(1);
        }

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        new KafkaConsumer<String, String>(props).close();
        System.out.println(
                "java client accepted " + (props.size() - 2) + " configs");
    }
}
