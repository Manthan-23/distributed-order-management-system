import axios from "axios";

async function callPaymentService(orderId, amount) {
  const response = await axios.post("http://localhost:4000/pay", {
    orderId,
    amount
  });

  return response.data.status;
}

export default callPaymentService;