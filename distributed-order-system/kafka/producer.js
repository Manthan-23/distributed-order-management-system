import {Kafka} from "kafkajs";

const kafka = new Kafka({
    clientId: "order-service",
    // brokers: ["localhost:9092"],
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    connectionTimeout: 10000
});

export const producer = kafka.producer();

export async function connectProducer(){
    await producer.connect();
}

export async function sendOrderCreatedEvent(order){
    await producer.send({
        topic: "order.created",
        messages: [
            {
                key: String(order.orderId),
                value: JSON.stringify(order),
            },
        ],
    });
}