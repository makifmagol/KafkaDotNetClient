using System;
using Confluent.Kafka;

namespace KafkaTest
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var config = new ConsumerConfig { GroupId = "product-consumer", BootstrapServers = "kafka:9092"};
                using (var consumer = new ConsumerBuilder<string, string>(config).Build())
                {
                    consumer.Subscribe("127.0.0.1.dbo.Product");
                    while (true)
                    {
                        ConsumeResult<string, string> consumeResult = consumer.Consume();
                        Console.WriteLine($"Message {consumeResult.TopicPartitionOffset}:{consumeResult.Value}");
                    consumer.Commit();
                }
            }
            }
            catch (System.Exception ex)
            {
            Console.WriteLine(ex.Message);
            }
        }
    }
}
