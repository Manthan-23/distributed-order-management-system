// Mock PostgreSQL pool
jest.mock('../connectDB.js', () => ({
  default: {
    query: jest.fn().mockResolvedValue({ rows: [] })
  }
}));

// Mock Kafka producer
jest.mock('../kafka/producer.js', () => ({
  connectProducer: jest.fn(),
  sendMessage: jest.fn()   // rename if your function is named differently
}));

// Mock Kafka consumer
jest.mock('../kafka/consumer.js', () => ({
  startOrderConsumer: jest.fn()
}));