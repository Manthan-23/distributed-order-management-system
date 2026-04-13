import { Kafka } from "kafkajs";
import pool from "../connectDB.js";

const kafka = new Kafka({
  clientId: "order-service-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "order-status-group",
});

export async function startOrderConsumer() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "payment.success",
    fromBeginning: true,
  });

  await consumer.subscribe({
    topic: "payment.failed",
    fromBeginning: true,
  });

  console.log("Order Service consuming payment.success and payment.failed");

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      // const payload = JSON.parse(message.value.toString());

      let payload;

      try {
        const rawValue = message.value?.toString()?.trim();

        if (!rawValue) {
          console.log(`Skipping empty message on topic ${topic}`);
          return;
        }

        payload = JSON.parse(rawValue);
      } catch (err) {
        console.error(`Invalid JSON message on topic ${topic}:`, message.value?.toString());
        return;
      }
      const orderId = payload.orderId;
      const eventId = payload.eventId;

      const client = await pool.connect();



      try {
        await client.query("BEGIN");

        const eventRes = await client.query(
          "SELECT id FROM processed_events WHERE event_id = $1",
          [eventId]
        );

        if (eventRes.rows.length > 0) {
          console.log(`Duplicate event skipped: ${eventId}`);
          await client.query("COMMIT");
          return;
        }

        const orderRes = await client.query(
          "SELECT status FROM orders WHERE id = $1",
          [orderId]
        );

        const currentStatus = orderRes.rows[0]?.status;

        if (currentStatus === "COMPLETED" || currentStatus === "PAYMENT_FAILED") {
          console.log(`Skipping duplicate event for order ${orderId}`);
          await client.query("COMMIT");
          return;
        }

        if (topic === "payment.success") {
          console.log(`Payment success received for order ${orderId}`);

          // Optional intermediate state
          await client.query(
            "UPDATE orders SET status = 'PAID' WHERE id = $1",
            [orderId]
          );

          await client.query(
            "UPDATE orders SET status = 'COMPLETED' WHERE id = $1",
            [orderId]
          );

        }

        if (topic === "payment.failed") {
          console.log(`Payment failed received for order ${orderId}`);

          const itemsRes = await client.query(
            "SELECT product_id, quantity FROM order_items WHERE order_id = $1",
            [orderId]
          );

          for (const item of itemsRes.rows) {
            await client.query(
              "UPDATE products SET stock_qty = stock_qty + $1 WHERE id = $2",
              [item.quantity, item.product_id]
            );
          }

          await client.query(
            "UPDATE orders SET status = 'PAYMENT_FAILED' WHERE id = $1",
            [orderId]
          );
        }

        await client.query(
          "INSERT INTO processed_events (event_id, topic) VALUES ($1, $2)",
          [eventId, topic]
        );

        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        console.error("Error updating order status from Kafka event:", err.message);
      } finally {
        client.release();
      }
    },
  });
}