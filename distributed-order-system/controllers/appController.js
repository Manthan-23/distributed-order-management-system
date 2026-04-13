import pool from "../connectDB.js";
import callPaymentService from '../helpers/payment-helper.js'
import { sendOrderCreatedEvent } from "../kafka/producer.js";


function simulatePayment() {
    // 70% success, 30% failure
    return Math.random() < 0.7;
}


export async function createOrder(req, res) {

    const client = await pool.connect();


    let orderId;

    try {

        const { user_id, items } = req.body;

        await client.query("BEGIN");

        // 1. Create order as PENDING
        const orderResult = await client.query(
            "INSERT INTO orders (user_id, total_amount, status) VALUES ($1, $2, $3) RETURNING id",
            [user_id, 0, "PENDING"]
        );

        orderId = orderResult.rows[0].id;


        await client.query("COMMIT");




        await client.query("BEGIN");


        let totalAmount = 0;

        // 2. Insert order items
        for (const item of items) {
            const productRes = await client.query(
                "SELECT price, stock_qty FROM products WHERE id = $1",
                [item.product_id]
            );

            if (productRes.rows.length === 0) {
                throw new Error("Product not found");
            }

            const { price, stock_qty } = productRes.rows[0];

            if (stock_qty < item.quantity) {
                throw new Error("Insufficient stock");
            }

            // Reduce stock
            await client.query(
                "UPDATE products SET stock_qty = stock_qty - $1 WHERE id = $2",
                [item.quantity, item.product_id]
            );

            await client.query(
                "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)",
                [orderId, item.product_id, item.quantity, price]
            );

            totalAmount += price * item.quantity;

        }

        await client.query(
            "UPDATE orders SET total_amount = $1, status = $2 WHERE id = $3",
            [totalAmount, "PAYMENT_PENDING", orderId]
        );

        await sendOrderCreatedEvent({
            orderId,
            userId: user_id,
            amount: totalAmount,
        });
        

        await client.query("COMMIT");

        

        res.json({
            order_id: orderId,
            status: "PAYMENT_PENDING"
        });

    } catch (err) {
        await client.query("ROLLBACK");

        if (orderId) {
            await client.query(
                "UPDATE orders SET status = 'FAILED' WHERE id = $1",
                [orderId]
            );
        }

        console.error(err.message);
        res.status(400).json({ error: err.message });

    } finally {
        client.release();
    }
}



export async function getOrder(req, res) {
    const order_id = req.params.id;

    try {
        const getOrder = await pool.query(`
      select o.id as order_id, o.total_amount, o.status, o.created_at, ori.quantity, 
      ori.price, pd.id as product_id, pd.name  from orders as o
      JOIN order_items as ori on o.id = ori.order_id
      JOIN products as pd on ori.product_id = pd.id where o.id = $1`, [order_id]);

        const order = {
            order_id: getOrder.rows[0].order_id,
            status: getOrder.rows[0].status,
            total_amount: getOrder.rows[0].total_amount,
            items: getOrder.rows.map(row => ({
                product_id: row.product_id,
                name: row.name,
                quantity: row.quantity,
                price: row.price
            }))
        };

        res.json({ order });

    } catch (err) {
        console.error(err);
        res.status(500).json({ status: "error", log: "Error getting order" });
    }
}



export async function getProducts(req, res) {
    try {
        const getProds = await pool.query("SELECT * FROM products");
        res.json({ products: getProds.rows });
    } catch (err) {
        console.error(err);
        res.status(500).json({ status: "error", log: "Error getting products" });
    }
}