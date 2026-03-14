package consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import consumer.client.TransactionPostClient;
import consumer.model.Transaction;
import consumer.model.TransactionPostRequest;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class TransactionConsumerService {

    private final ObjectMapper objectMapper;
    private final TransactionPostClient postClient;
    private final String studentName;
    private final String studentCard;

    public TransactionConsumerService(ObjectMapper objectMapper,
                                      TransactionPostClient postClient,
                                      String studentName,
                                      String studentCard) {
        this.objectMapper = objectMapper;
        this.postClient = postClient;
        this.studentName = studentName;
        this.studentCard = studentCard;
        
    }

    public void startConsumer(Channel channel, String queue) throws Exception {
        DeliverCallback deliverCallback = (consumerTag, delivery) ->
                handleDelivery(channel, queue, delivery);

        CancelCallback cancelCallback = consumerTag ->
                System.out.println("Consumer cancelado para cola " + queue + " tag=" + consumerTag);

        channel.basicConsume(queue, false, deliverCallback, cancelCallback);
    }

    private void handleDelivery(Channel channel, String queue, Delivery delivery) {
        long tag = delivery.getEnvelope().getDeliveryTag();

        try {
            boolean success = processWithRetry(queue, delivery, 2);

            if (success) {
                channel.basicAck(tag, false);
                System.out.println("ACK enviado. Cola: " + queue + " Tag: " + tag);
            } else {
                channel.basicNack(tag, false, true);
                System.err.println("NACK enviado con requeue. Cola: " + queue + " Tag: " + tag);
            }

        } catch (Exception e) {
            try {
                channel.basicNack(tag, false, true);
            } catch (Exception nackEx) {
                System.err.println("Error enviando NACK: " + nackEx.getMessage());
            }
            System.err.println("Error procesando entrega. Cola: " + queue + " Error: " + e.getMessage());
        }
    }

    private boolean processWithRetry(String queue, Delivery delivery, int maxAttempts) {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean success = processMessage(queue, delivery, attempt);
            if (success) {
                return true;
            }
            System.err.println("Intento " + attempt + " fallido para cola " + queue);
        }
        return false;
    }

    private boolean processMessage(String queue, Delivery delivery, int attempt) {
        try {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            Transaction transaction = objectMapper.readValue(payload, Transaction.class);

            if (transaction == null) {
                System.err.println("Mensaje nulo recibido en cola " + queue);
                return false;
            }

            String originalId = safe(transaction.getIdTransaccion());
            String uniqueId = originalId + "-" + UUID.randomUUID();

            TransactionPostRequest request = new TransactionPostRequest();
            request.setIdTransaccion(uniqueId);
            request.setMonto(transaction.getMonto());
            request.setMoneda(transaction.getMoneda());
            request.setCuentaOrigen(transaction.getCuentaOrigen());
            request.setBancoDestino(transaction.getBancoDestino());
            request.setDetalle(transaction.getDetalle());
            request.setNombre("Mario Jose Barrera");
            request.setCarnet("0905-23-13800");

            System.out.println("Procesando transacción. Cola: " + queue
                    + " | Intento: " + attempt
                    + " | ID original: " + originalId
                    + " | ID nuevo: " + uniqueId);

            return postClient.sendTransaction(request);

        } catch (Exception e) {
            System.err.println("Error en processMessage. Cola: " + queue
                    + " | Intento: " + attempt
                    + " | Error: " + e.getMessage());
            return false;
        }
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }
}