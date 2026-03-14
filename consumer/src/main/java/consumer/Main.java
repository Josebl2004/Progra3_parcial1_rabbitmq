package consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import consumer.client.TransactionPostClient;
import consumer.config.RabbitMQConfig;
import consumer.service.TransactionConsumerService;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Main {

    private static final Set<String> BANK_QUEUES = Set.of("BANRURAL", "GYT", "BAC", "BI");
    private static final String POST_URL =
            "https://7e0d9ogwzd.execute-api.us-east-1.amazonaws.com/default/guardarTransacciones";

    public static void main(String[] args) throws Exception {
        String rabbitHost = System.getenv().getOrDefault("RABBIT_HOST", "localhost");
        int rabbitPort = Integer.parseInt(System.getenv().getOrDefault("RABBIT_PORT", "5673"));
        String rabbitUser = System.getenv().getOrDefault("RABBIT_USER", "admin");
        String rabbitPassword = System.getenv().getOrDefault("RABBIT_PASSWORD", "admin");

        String studentName = System.getenv().getOrDefault("STUDENT_NAME", "Jose Barrera");
        String studentCard = System.getenv().getOrDefault("STUDENT_CARD", "202400000");

        RabbitMQConfig rabbitMQConfig = new RabbitMQConfig(rabbitHost, rabbitPort, rabbitUser, rabbitPassword);

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        ObjectMapper objectMapper = new ObjectMapper();

        TransactionPostClient postClient = new TransactionPostClient(httpClient, objectMapper, POST_URL);
        TransactionConsumerService consumerService =
                new TransactionConsumerService(objectMapper, postClient, studentName, studentCard);

        Connection connection = rabbitMQConfig.createConnection();
        Channel channel = connection.createChannel();

        channel.basicQos(1);
        
        channel.queueDeclare("cola_duplicados", true, false, false, null);


        for (String queue : BANK_QUEUES) {
            channel.queueDeclare(queue, true, false, false, null);
            consumerService.startConsumer(channel, queue);
        }

        System.out.println("Consumer activo en colas: " + BANK_QUEUES);
        new CountDownLatch(1).await();
    }
}