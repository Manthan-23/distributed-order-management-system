import e from "express";
import { connectProducer } from "./kafka/producer.js";
import { startPaymentConsumer } from "./kafka/consumer.js";

const app = e();

app.use(e.json());





// app.post("/pay", (req, res) => {
//   const success = Math.random() < 0.7;

//   if (!success) {
//     return res.status(402).json({ status: "PAYMENT_FAILED" });
//   }

//   res.json({ status: "PAID" });
// });

(async () => {
  try {
    await connectProducer();
    await startPaymentConsumer();
  } catch (err) {
    console.error("Payment service startup failed", err);
  }
})();



// app.listen(4000, () => {
//   console.log("Payment service running on port 4000");
// });