using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MyJetWallet.MatchingEngine.EventReader.BaseReader
{
    public abstract class BaseBatchQueueReader<T>
    {
        private readonly TimeSpan _reconnectTimeoutSeconds = TimeSpan.FromSeconds(30);
        private readonly TimeSpan _timeoutBeforeStop = TimeSpan.FromSeconds(10);

        private readonly string _connectionString;
        private readonly int _prefetchCount;
        private readonly int _batchCount;
        private readonly ILogger<BaseBatchQueueReader<T>> _logger;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _queueReaderTask;

        protected ConcurrentQueue<CustomQueueItem<T>> Queue;

        protected abstract string ExchangeName { get; }
        protected abstract string QueueName { get; }
        protected abstract string[] RoutingKeys { get; }
        protected abstract bool IsQueueAutoDelete { get; }
        protected abstract Task ProcessBatch(IList<CustomQueueItem<T>> batch);
        protected abstract void LogQueue();
        protected abstract T DeserializeMessage(ReadOnlyMemory<byte> body, string routingKey);


        protected EventHandler<BasicDeliverEventArgs> CreateOnMessageReceivedEventHandler(IModel channel)
        {
            void OnMessageReceived(object o, BasicDeliverEventArgs basicDeliverEventArgs)
            {
                try
                {
                    T message = DeserializeMessage(basicDeliverEventArgs.Body, basicDeliverEventArgs.RoutingKey);

                    if (message != null)
                    {
                        Queue.Enqueue(new CustomQueueItem<T>(message, basicDeliverEventArgs.DeliveryTag, channel));
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Unexpected exception");

                    channel.BasicReject(basicDeliverEventArgs.DeliveryTag, false);
                }
            }

            return OnMessageReceived;
        }

        protected BaseBatchQueueReader(
            string connectionString,
            int prefetchCount,
            int batchCount,
            ILogger<BaseBatchQueueReader<T>> logger)
        {
            _connectionString = connectionString;
            _prefetchCount = prefetchCount;
            _batchCount = batchCount;
            _logger = logger;
        }

        public void Start()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            Queue = new ConcurrentQueue<CustomQueueItem<T>>();
            _queueReaderTask = StartRabbitSessionWithReconnects();
        }

        public void Stop()
        {
            _logger.LogInformation("Stopping  queue reader");
            _cancellationTokenSource.Cancel();

            int index = Task.WaitAny(_queueReaderTask, Task.Delay(_timeoutBeforeStop));

            _logger.LogInformation(index == 0 ? "Queue reader stopped" : $"Unable to stop queue reader within {_timeoutBeforeStop.TotalSeconds} seconds.");
        }

        private async Task StartRabbitSessionWithReconnects()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    await StartQueueReader();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
                finally
                {
                    if (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        _logger.LogInformation($"Connection will be reconnected in {_reconnectTimeoutSeconds} seconds");
                        await Task.Delay(_reconnectTimeoutSeconds);
                    }
                }
            }

            _logger.LogInformation("Session was closed");
        }

        private async Task StartQueueReader()
        {
            var factory = new ConnectionFactory
            {
                Uri = new Uri(_connectionString)
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.BasicQos(0, (ushort)_prefetchCount, false);

                channel.QueueDeclare(QueueName, true, false, IsQueueAutoDelete);

                if (RoutingKeys.Any())
                {
                    foreach (var key in RoutingKeys)
                        channel.QueueBind(QueueName, ExchangeName, key);
                }
                else
                {
                    channel.QueueBind(QueueName, ExchangeName, "");
                }
                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += CreateOnMessageReceivedEventHandler(channel);

                var tag = channel.BasicConsume(QueueName, false, consumer);

                var writerTask = StartHandler(connection);

                while (!_cancellationTokenSource.IsCancellationRequested && connection.IsOpen)
                    await Task.Delay(100);

                await writerTask;

                channel.BasicCancel(tag);
                connection.Close();

                if (Queue.Count > 0)
                {
                    _logger.LogWarning("Queue is not empty on shutdown!");
                    LogQueue();
                }

                Queue.Clear();
            }
        }

        private async Task StartHandler(IConnection connection)
        {
            while ((!_cancellationTokenSource.IsCancellationRequested || Queue.Count > 0) && connection.IsOpen)
            {
                var isFullBatch = false;
                try
                {
                    var exceptionThrowed = false;
                    var list = new List<CustomQueueItem<T>>();
                    try
                    {
                        for (var i = 0; i < _batchCount; i++)
                            if (Queue.TryDequeue(out var customQueueItem))
                                list.Add(customQueueItem);
                            else
                                break;

                        if (list.Count > 0)
                        {
                            isFullBatch = list.Count == _batchCount;

                            await ProcessBatch(list);
                        }
                    }
                    catch (Exception e)
                    {
                        exceptionThrowed = true;

                        _logger.LogError(e, "Error processing batch");

                        foreach (var item in list)
                            item.Reject();
                    }
                    finally
                    {
                        if (!exceptionThrowed)
                            foreach (var item in list)
                                item.Accept();
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Unexpected exception");
                }
                finally
                {
                    await Task.Delay(isFullBatch ? 1 : 1000);
                }
            }
        }
    }
}