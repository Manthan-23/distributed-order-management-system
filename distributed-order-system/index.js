import express from "express";
import { Router } from "express";
import pool from "./connectDB.js";
import orderRoutes from "./routes/order.route.js";
import productRoutes from "./routes/product.route.js";
import { connectProducer } from "./kafka/producer.js";
import { startOrderConsumer } from "./kafka/consumer.js";

const router = Router();

const app = express();
// app.use("/api", router);
app.use(express.json());

const PORT = 7070;


app.use("/order", orderRoutes);
app.use("/product", productRoutes);

export default app;

app.get("/health", async (req, res) => {
  try {
    const result = await pool.query("SELECT 1");
    res.json({ status: "ok", db: "connected" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ status: "error", db: "not connected" });
  }
});



// Added [1] here so Node.js can correctly match the executed file
if (process.env.NODE_ENV !== 'test') {
  (async () => {
    try {
      await connectProducer();
      await startOrderConsumer();
      console.log("Order Service Kafka ready");
      app.listen(PORT, () => {
        console.log(`🚀 Server listening on http://localhost:${PORT}`);
      });
    } catch (err) {
      console.error("Order Service Kafka startup failed:", err);
      process.exit(1);
    }
  })();
}


// (async () => {
//   try {
//     await connectProducer();
//     await startOrderConsumer();
//     console.log("Order Service Kafka ready");
//     app.listen(7070, () => {
//       console.log("🚀 Server listening on http://localhost:7070");
//     });

//   } catch (err) {
//     console.error("Order Service Kafka startup failed:", err);
//   }
// })();



// (async () => {
//   try {
//     await connectProducer();
//     console.log('✅ Kafka Producer connected');

//     app.listen(PORT, () => {
//       console.log(`🚀 Server listening on http://localhost:${PORT}`);
//     });
//   } catch (err) {
//     console.error('❌ Failed to start server due to Kafka connection error:', err);
//     process.exit(1); // Exit if critical infrastructure is down
//   }
// })();