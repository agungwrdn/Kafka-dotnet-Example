using Confluent.Kafka;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Contoh_Kafka
{
    public class Program
    {
        
        static void Main(string[] args)
        {
            IProducer<Null, string> producer;
            var conf = new ConsumerConfig
            {
                GroupId = "insert-tnt",
                BootstrapServers = "localhost:19092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Acks = Acks.All
            };
            producer = new ProducerBuilder<Null, string>(conf).Build();
            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                var topic = "insert-tnt";
                c.Subscribe("insert-tnt");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        var cr = c.Consume(cts.Token);
                        try
                        {
                            c.Commit(cr);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'. Berhasil");
                        }
                        catch (ConsumeException e)
                        {
                            c.Seek(cr.TopicPartitionOffset);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}