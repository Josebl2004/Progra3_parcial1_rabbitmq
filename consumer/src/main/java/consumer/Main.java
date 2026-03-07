package consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Set<String> BANK_QUEUES = Set.of("BANRURAL", "GYT", "BAC", "BI");
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String rabbitHost = System.getenv().getOrDefault("RABBIT_HOST", "localhost");
        int rabbitPort = Integer.parseInt(System.getenv().getOrDefault("RABBIT_PORT", "5672"));
        String rabbitUser = System.getenv().getOrDefault("RABBIT_USER", "guest");
        String rabbitPassword = System.getenv().getOrDefault("RABBIT_PASSWORD", "guest");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(rabbitUser);
        factory.setPassword(rabbitPassword);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        for (String queue : BANK_QUEUES) {
            channel.queueDeclare(queue, true, false, false, null);
        }

        for (String queue : BANK_QUEUES) {
            startConsumer(channel, queue);
        }

        System.out.println("Consumer activo en colas: " + BANK_QUEUES);
        new CountDownLatch(1).await();
    }

    private static void startConsumer(Channel channel, String queue) throws Exception {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            processMessage(queue, delivery);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        CancelCallback cancelCallback = consumerTag ->
                System.out.println("Consumer cancelado para cola " + queue + " tag=" + consumerTag);

        channel.basicConsume(queue, false, deliverCallback, cancelCallback);
    }

    private static void processMessage(String queue, Delivery delivery) {
        try {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            JsonNode tx = mapper.readTree(payload);

            String id = tx.path("idTransaccion").asText("");
            String bank = tx.path("bancoDestino").asText("");
            double amount = tx.path("monto").asDouble(0.0);
            String beneficiary = tx.path("detalle").path("nombreBeneficiario").asText("");

            System.out.printf(
                    "[%s] Recibida transaccion id=%s banco=%s monto=%.2f beneficiario=%s%n",
                    queue, id, bank, amount, beneficiary
            );
        } catch (Exception ex) {
            System.err.println("Error procesando mensaje de cola " + queue + ": " + ex.getMessage());
        }
    }
}
