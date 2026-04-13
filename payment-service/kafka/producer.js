import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9092"],
});

export const producer = kafka.producer();

export async function connectProducer() {
  await producer.connect();
  console.log("Payment Service Kafka Producer connected");
}

export async function publishPaymentResult(topic, payload) {
  await producer.send({
    topic,
    messages: [
      {
        key: String(payload.orderId),
        value: JSON.stringify(payload),
      },
    ],
  });
}
