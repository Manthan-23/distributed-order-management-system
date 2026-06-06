import { processPaymentMessage } from '../kafka/consumer.js';
import { publishPaymentResult } from '../kafka/producer.js';

describe('Payment Consumer - processPaymentMessage', () => {

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should skip empty messages', async () => {
    const result = await processPaymentMessage({
      topic: 'order.created',
      message: { value: null }
    });
    expect(result).toEqual({ skipped: true });
    expect(publishPaymentResult).not.toHaveBeenCalled();
  });

  it('should handle invalid JSON gracefully', async () => {
    const result = await processPaymentMessage({
      topic: 'order.created',
      message: { value: Buffer.from('not-valid-json') }
    });
    expect(result).toEqual({ error: true });
    expect(publishPaymentResult).not.toHaveBeenCalled();
  });

  it('should publish payment.success on successful payment', async () => {
    jest.spyOn(Math, 'random').mockReturnValue(0.1); // 0.1 < 0.9 = success

    const result = await processPaymentMessage({
      topic: 'order.created',
      message: {
        value: Buffer.from(JSON.stringify({
          orderId: 'order-123',
          amount: 500
        }))
      }
    });

    expect(publishPaymentResult).toHaveBeenCalledWith(
      'payment.success',
      expect.objectContaining({ orderId: 'order-123' })
    );
    expect(result).toEqual({ status: 'PAYMENT_SUCCESS' });
  });

  it('should publish payment.failed on failed payment', async () => {
    jest.spyOn(Math, 'random').mockReturnValue(0.95); // 0.95 > 0.9 = failure

    const result = await processPaymentMessage({
      topic: 'order.created',
      message: {
        value: Buffer.from(JSON.stringify({
          orderId: 'order-456',
          amount: 300
        }))
      }
    });

    expect(publishPaymentResult).toHaveBeenCalledWith(
      'payment.failed',
      expect.objectContaining({
        orderId: 'order-456',
        reason: 'PAYMENT_DECLINED'
      })
    );
    expect(result).toEqual({ status: 'PAYMENT_FAILED' });
  });

});