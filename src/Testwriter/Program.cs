using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Unicode;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Testwriter
{
    class Program
    {
        public static int _prefetchCount = 100;

        static void Main(string[] args)
        {
            const string QueueName = "test-query";


            var factory = new ConnectionFactory
            {
                Uri = new Uri("amqp://alex:123456@localhost:5672")
            };

            Task.Run(() =>
            {
                using (var connection1 = factory.CreateConnection())
                using (var channel1 = connection1.CreateModel())
                {
                    channel1.ExchangeDeclare("alex", "direct", true, false);

                    while (true)
                    {
                        channel1.BasicPublish("alex", "", body: Encoding.UTF8.GetBytes($"{DateTime.Now:O}"));
                        Thread.Sleep(100);
                    }
                }
            });

            var loger = LoggerFactory.Create(builder => { builder.AddConsole(); });


            var reader = new StringQueueReader("amqp://alex:123456@localhost:5672", 10, 10,
                loger.CreateLogger<StringQueueReader>());

            reader.Start();

            Console.ReadLine();

            reader.Stop();
        }

        private static EventHandler<BasicDeliverEventArgs> CreateOnMessageReceivedEventHandler(IModel channel)
        {
            void OnMessageReceived(object o, BasicDeliverEventArgs basicDeliverEventArgs)
            {
                try
                {
                    var message = Encoding.UTF8.GetString(basicDeliverEventArgs.Body.ToArray());

                    Console.WriteLine($"message: {message}");

                    channel.BasicAck(basicDeliverEventArgs.DeliveryTag, false);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    channel.BasicReject(basicDeliverEventArgs.DeliveryTag, false);
                }
            }

            return OnMessageReceived;
        }


    }


    public class StringQueueReader : BaseBatchQueueReader<string>
    {
        public StringQueueReader(string connectionString, int prefetchCount, int batchCount, ILogger<BaseBatchQueueReader<string>> logger) : base(connectionString, prefetchCount, batchCount, logger)
        {
        }

        protected override string ExchangeName { get; } = "alex";
        protected override string QueueName { get; } = "alex-test";
        protected override bool IsQueueAutoDelete { get; } = true;
        protected override string[] RoutingKeys { get; } = {};

        protected override async Task ProcessBatch(IList<CustomQueueItem<string>> batch)
        {
            Console.WriteLine("-------------------");
            foreach (var item in batch)
            {
                Console.WriteLine(item.Value);
            }
            Console.WriteLine("^^^^^^^^^^^^^^^^^^^");

            await Task.Delay(1000);
        }

        protected override void LogQueue()
        {
        }

        protected override string DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            return Encoding.UTF8.GetString(body.ToArray());
        }
    }

    
}
