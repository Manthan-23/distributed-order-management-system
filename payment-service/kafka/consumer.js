import { Kafka } from "kafkajs";
import { publishPaymentResult } from "./producer.js";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "payment-group",
});

// Fake payment logic
function simulatePayment() {
  return Math.random() < 0.9; // 70% success
  // return false;
}

export async function startPaymentConsumer() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "order.created",
    fromBeginning: true,
  });

  console.log("Payment Service consuming order.created");

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      // const order = JSON.parse(message.value.toString());

      let order;

      try {
        const rawValue = message.value?.toString()?.trim();

        if (!rawValue) {
          console.log(`Skipping empty message on topic ${topic}`);
          return;
        }

        order = JSON.parse(rawValue);
      } catch (err) {
        console.error(`Invalid JSON message on topic ${topic}:`, message.value?.toString());
        return;
      }

      console.log("Processing payment for order:", order.orderId);

      const success = simulatePayment();

      if (!success) {
        await publishPaymentResult("payment.failed", {
          eventId: `payment-failed-${order.orderId}`,
          orderId: order.orderId,
          reason: "PAYMENT_DECLINED",
        });

        console.log("Payment failed:", order.orderId);
        return;
      }

      await publishPaymentResult("payment.success", {
        eventId: `payment-success-${order.orderId}`,
        orderId: order.orderId,
        amount: order.amount,
      });

      console.log("Payment successful:", order.orderId);
    },
  });
}
