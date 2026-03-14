package consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import consumer.client.TransactionPostClient;
import consumer.model.Transaction;
import consumer.model.TransactionPostRequest;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class TransactionConsumerService {

    private final ObjectMapper objectMapper;
    private final TransactionPostClient postClient;
    private final String studentName;
    private final String studentCard;
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();
    
    private void sendToDuplicateQueue(Channel channel, Transaction transaction) throws Exception {
        byte[] payload = objectMapper.writeValueAsString(transaction).getBytes(StandardCharsets.UTF_8);

        channel.queueDeclare("cola_duplicados", true, false, false, null);
        channel.basicPublish("", "cola_duplicados", MessageProperties.PERSISTENT_TEXT_PLAIN, payload);
    }



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
        	boolean success = processWithRetry(channel, queue, delivery, 2);

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

    private boolean processWithRetry(Channel channel, String queue, Delivery delivery, int maxAttempts) {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean success = processMessage(channel, queue, delivery, attempt);
            if (success) {
                return true;
            }
            System.err.println("Intento " + attempt + " fallido para cola " + queue);
        }
        return false;
    }


    private boolean processMessage(Channel channel, String queue, Delivery delivery, int attempt) {
        try {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            Transaction transaction = objectMapper.readValue(payload, Transaction.class);

            if (transaction == null) {
                System.err.println("Mensaje nulo recibido en cola " + queue);
                return false;
            }

            String originalId = safe(transaction.getIdTransaccion());

            if (originalId.isEmpty()) {
                System.err.println("ID de transacción vacío. Cola: " + queue);
                return false;
            }

            if (!processedIds.add(originalId)) {
                sendToDuplicateQueue(channel, transaction);
                logEstado(originalId, "DUPLICADA", "cola_duplicados");
                return true;
            }

            String uniqueId = originalId + "-" + UUID.randomUUID();

            TransactionPostRequest request = new TransactionPostRequest();
            request.setIdTransaccion(uniqueId);
            request.setMonto(transaction.getMonto());
            request.setMoneda(transaction.getMoneda());
            request.setCuentaOrigen(transaction.getCuentaOrigen());
            request.setBancoDestino(transaction.getBancoDestino());
            request.setDetalle(transaction.getDetalle());
            request.setNombre(studentName);
            request.setCarnet(studentCard);

            System.out.println("Procesando transacción. Cola: " + queue
                    + " | Intento: " + attempt
                    + " | ID original: " + originalId
                    + " | ID nuevo: " + uniqueId);

            boolean postSuccess = postClient.sendTransaction(request);

            if (postSuccess) {
                logEstado(originalId, "PROCESADA", queue);
            }

            return postSuccess;

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