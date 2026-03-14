Link de video: https://drive.google.com/file/d/1U1IzpqDTfTtKrOJ2zSbuatXcYqLRfUCF/view?usp=drive_link

# Procesamiento de Transacciones Bancarias con RabbitMQ y Java

# Descripción del Proyecto

Este proyecto implementa un sistema distribuido para el procesamiento de transacciones bancarias utilizando RabbitMQ como sistema de colas de mensajería y Java + Maven como tecnología principal.

El sistema desacopla la generación y el procesamiento de transacciones, permitiendo que múltiples entidades bancarias procesen sus operaciones de manera independiente.

Flujo general:

1. Obtener transacciones desde una API externa (GET)
2. Publicar cada transacción en RabbitMQ según su banco destino
3. Consumir las transacciones desde colas independientes
4. Enviar cada transacción a otra API (POST) para almacenarla

# Arquitectura del Sistema

El sistema implementa el patrón Producer–Consumer utilizando RabbitMQ como intermediario.

API (GET)
   |
Producer (Java)
   |
RabbitMQ (colas por banco)
   |
Consumer (Java)
   |
API (POST)

# Componentes del Sistema

# Producer

Responsable de obtener las transacciones desde la API externa y enviarlas a RabbitMQ.

Funciones:

- Consumir endpoint GET /transacciones
- Parsear el JSON recibido
- Recorrer el arreglo transacciones
- Leer el campo bancoDestino
- Publicar cada transacción en una cola con el nombre del banco

Ejemplo:

bancoDestino = BAC

Se publica en la cola:

BAC

# RabbitMQ

RabbitMQ funciona como middleware de mensajería.

Se utilizan colas independientes por banco:

BAC
BANRURAL
BI
GYT

Características:

- Colas durables
- Mensajes persistentes
- Procesamiento asíncrono

# Consumer

El Consumer escucha múltiples colas y procesa las transacciones.

Funciones:

- Escuchar varias colas
- Deserializar el JSON
- Modificar idTransaccion
- Agregar nombre y carnet
- Enviar la transacción a la API POST

Endpoint utilizado:

POST /guardarTransacciones

# Flujo completo

1. Producer consume GET /transacciones
2. API devuelve lote de transacciones
3. Producer publica cada transacción en RabbitMQ
4. RabbitMQ distribuye mensajes por banco
5. Consumer escucha múltiples colas
6. Consumer procesa cada mensaje
7. Consumer agrega UUID al idTransaccion
8. Consumer agrega nombre y carnet
9. Consumer envía POST a la API
10. API guarda la transacción en la base de datos
11. Consumer confirma el mensaje con ACK

# Modificación de Transacciones

Antes de enviar la transacción al POST, el Consumer:

1. Genera un UUID y lo agrega al idTransaccion

Ejemplo:

Original:
TX-10031

Modificado:
TX-10031-4f5ed8dd-400b-4475-8009-3f3ab02cc095

2. Agrega información del estudiante:

nombre
carnet

Ejemplo JSON enviado:

{
"idTransaccion":"TX-10031-UUID",
"monto":3050.4,
"moneda":"GTQ",
"cuentaOrigen":"001-100031-7",
"bancoDestino":"BI",
"detalle": {...},
"nombre":"Mario Jose Barrera",
"carnet":"0905-23-13800"
}

# Garantía de no pérdida de mensajes

Se utilizan:

Colas durables:

channel.queueDeclare(bank, true, false, false, null);

Mensajes persistentes:

MessageProperties.PERSISTENT_TEXT_PLAIN

ACK manual:

POST exitoso -> ACK
POST fallido -> no ACK

# Manejo de errores

El Consumer implementa reintentos:

Intento 1
Intento 2

Si falla nuevamente:

NACK con requeue

# Tecnologías Utilizadas

Java 17
Maven
RabbitMQ
Docker
Jackson (JSON)
Java HttpClient
REST APIs

# Estructura del Proyecto

Producer

producer
config
RabbitMQConfig
client
TransactionApiClient
service
TransactionPublisherService
model
Transaction
TransactionBatch
TransactionDetail
TransactionReferences
Main

Consumer

consumer
config
RabbitMQConfig
client
TransactionPostClient
service
TransactionConsumerService
model
Transaction
TransactionDetail
TransactionPostRequest
Main

# Cómo ejecutar el proyecto

1. Iniciar RabbitMQ con Docker

docker run -d -p 5673:5672 -p 15673:15672 rabbitmq:3-management

Panel web:

http://localhost:15673

Usuario:
guest
guest

2. Ejecutar Producer

mvn exec:java -Dexec.mainClass="producer.Main"

3. Ejecutar Consumer

mvn exec:java -Dexec.mainClass="consumer.Main"

# Verificación del sistema

Abrir RabbitMQ:

http://localhost:15673

Ir a:

Queues and Streams

Se observarán las colas:

BAC
BANRURAL
BI
GYT

# Conclusión

El sistema demuestra el uso de RabbitMQ para desacoplar servicios y procesar transacciones bancarias de forma distribuida, segura y escalable.
