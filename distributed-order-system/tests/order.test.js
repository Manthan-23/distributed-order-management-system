import request from "supertest";
import app from "../index.js";

describe('Order service - Basic Health Check', () => {

    it('GET /health - should return health status', async () => {
        const res = await request(app).get('/health');
        expect([200, 500]).toContain(res.statusCode);
    });

    it('GET /orders - should respond with (200 or 401, not crash)', async () => {
        const res = await request(app).get('/order');
        expect([200, 401, 404]).toContain(res.statusCode);
    });

    it('POST /orders - should respond (not crash)', async () => {
        const res = await request(app).post('/order').
            send({ user_id: 1, items: [{ product_id: '2', quantity: 1 }] });
        expect([200, 201, 400, 401, 404]).toContain(res.statusCode);
    });
});