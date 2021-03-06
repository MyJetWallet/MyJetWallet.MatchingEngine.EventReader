﻿using System;
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
        private readonly TimeSpan _reconnectTimeoutSeconds = TimeSpan.FromSeconds(10);
        private readonly TimeSpan _timeoutBeforeStop = TimeSpan.FromSeconds(10);

        private readonly string _connectionString;
        private readonly int _prefetchCount;
        private readonly int _batchCount;
        private readonly ILogger<BaseBatchQueueReader<T>> _logger;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _queueReaderTask;
        private bool _isActive = false;

        //protected ConcurrentQueue<CustomQueueItem<T>> Queue;
        protected List<CustomQueueItem<T>> Data;
        protected object _sync = new object();

        protected abstract string ExchangeName { get; }
        protected abstract string QueueName { get; }
        protected abstract string[] RoutingKeys { get; }
        protected abstract bool IsQueueAutoDelete { get; }
        protected abstract Task ProcessBatch(IList<CustomQueueItem<T>> batch);
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
                        lock (_sync)
                        {
                            Data.Add(new CustomQueueItem<T>(message, basicDeliverEventArgs.DeliveryTag, channel));
                        }
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
            
            lock(_sync) Data = new List<CustomQueueItem<T>>();
            
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
            lock (_sync)
            {
                Data = new List<CustomQueueItem<T>>();
                _isActive = true;
            }

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
            }
        }

        private async Task StartHandler(IConnection connection)
        {
            while ((!_cancellationTokenSource.IsCancellationRequested) && connection.IsOpen)
            {
                try
                {
                    try
                    {
                        List<CustomQueueItem<T>> list = null;
                        lock (_sync)
                        {
                            if (_isActive && Data.Count > 0)
                            {
                                list = Data;
                                Data = new List<CustomQueueItem<T>>();
                            }
                        }

                        if (list != null)
                        {
                            var index = 0;
                            while (index < list.Count)
                            {
                                var batch = list.Skip(index).Take(_batchCount).ToList();
                                await ProcessBatch(batch);
                                index += batch.Count;
                            }

                            foreach (var item in list)
                                item.Accept();
                        }
                    }
                    catch (Exception e)
                    {
                        lock (_sync) _isActive = false;

                        connection.Close();
                        
                        _logger.LogError(e, "Error processing batch");
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Unexpected exception");
                }
                finally
                {
                    await Task.Delay(5);
                }
            }
        }
    }
}