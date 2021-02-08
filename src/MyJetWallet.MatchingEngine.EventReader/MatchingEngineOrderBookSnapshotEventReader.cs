using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchinEngine.Events.Models;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineOrderBookSnapshotEventReader : BaseBatchQueueReader<OrderBookSnapshotEvent>
    {
        private readonly ILogger<MatchingEngineOrderBookSnapshotEventReader> _logger;
        private readonly IMatchingEngineSubscriber<OrderBookSnapshotEvent>[] _subscribers;

        public MatchingEngineOrderBookSnapshotEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<OrderBookSnapshotEvent>[] subscribers,
            ILogger<MatchingEngineOrderBookSnapshotEventReader> logger)
            : base(settings.RabbitMqConnectionString, settings.PrefetchCount, settings.BatchCount, logger)
        {
            _logger = logger;
            _subscribers = subscribers;
            ExchangeName = settings.TopicName;
            QueueName = settings.QueryName;
        }

        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override string[] RoutingKeys { get; } = { MessageType.OrderBookSnapshot.ToString() };

        protected override async Task ProcessBatch(IList<CustomQueueItem<OrderBookSnapshotEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle OrderBookSnapshotEvent's'");
                    throw;
                }
            }
        }

        protected override void LogQueue()
        {
        }

        protected override OrderBookSnapshotEvent DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            var item = OrderBookSnapshotEvent.Parser.ParseFrom(body.ToArray());
            return item;
        }
    }
}