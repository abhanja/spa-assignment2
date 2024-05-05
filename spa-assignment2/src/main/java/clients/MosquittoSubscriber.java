package clients;

import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MosquittoSubscriber implements MqttCallback {
    private final int qos = 1;
    private String host = "tcp://localhost:1883";
    private String clientId = "Assignment2";
    private String topic = "test/topic";
    private String kafka_topic = "topic_0";
    private MqttClient client;

    private KafkaProducer<String, String> producer;

    public MosquittoSubscriber(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void start() throws MqttException {
        MqttConnectOptions conOpt = new MqttConnectOptions();
        conOpt.setCleanSession(true);

        final String uuid = UUID.randomUUID().toString().replace("-", "");

        String clientId = this.clientId + "-" + uuid;
        this.client = new MqttClient(this.host, clientId, new MemoryPersistence());
        this.client.setCallback(this);
        this.client.connect(conOpt);

        this.client.subscribe(this.topic, this.qos);
    }

    public void connectionLost(Throwable cause) {
        System.out.println("Connection lost because: " + cause);
        System.exit(1);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void messageArrived(String topic, MqttMessage message) throws MqttException {
        System.out.println(String.format("[%s] %s", topic, new String(message.getPayload())));
        final String key = topic;
        final String value = new String(message.getPayload());
        final ProducerRecord<String, String> record = new ProducerRecord<>(this.kafka_topic, key, value);
        producer.send(record);
    }
}
