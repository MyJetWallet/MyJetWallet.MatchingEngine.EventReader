using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchinEngine.Events.Models;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineReservedBalanceUpdateEventReader : BaseBatchQueueReader<ReservedBalanceUpdateEvent>
    {
        private readonly ILogger<MatchingEngineReservedBalanceUpdateEventReader> _logger;
        private readonly IMatchingEngineSubscriber<ReservedBalanceUpdateEvent>[] _subscribers;

        public MatchingEngineReservedBalanceUpdateEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<ReservedBalanceUpdateEvent>[] subscribers,
            ILogger<MatchingEngineReservedBalanceUpdateEventReader> logger)
            : base(settings.RabbitMqConnectionString, settings.PrefetchCount, settings.BatchCount, logger)
        {
            _logger = logger;
            _subscribers = subscribers;
            ExchangeName = settings.TopicName;
            QueueName = settings.QueryName;
            IsQueueAutoDelete = settings.IsQueueAutoDelete;
        }

        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override bool IsQueueAutoDelete { get; }
        protected override string[] RoutingKeys { get; } = { MessageType.ReservedBalanceUpdate.ToString() };

        protected override async Task ProcessBatch(IList<CustomQueueItem<ReservedBalanceUpdateEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle ReservedBalanceUpdateEvent's'");
                    throw;
                }
            }
        }

        protected override void LogQueue()
        {
        }

        protected override ReservedBalanceUpdateEvent DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            var item = ReservedBalanceUpdateEvent.Parser.ParseFrom(body.ToArray());
            return item;
        }
    }
}