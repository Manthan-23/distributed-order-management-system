jest.mock('kafkajs', () => {
  const mockConsumer = {
    connect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn()
  };
  const mockKafka = {
    consumer: jest.fn(() => mockConsumer)
  };
  return { Kafka: jest.fn(() => mockKafka) };
});

jest.mock('../kafka/producer.js', () => ({
  publishPaymentResult: jest.fn().mockResolvedValue(undefined)
}));