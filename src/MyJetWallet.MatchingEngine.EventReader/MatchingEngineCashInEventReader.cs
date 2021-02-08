using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchinEngine.Events.Models;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineCashInEventReader: BaseBatchQueueReader<CashInEvent>
    {
        private readonly ILogger<MatchingEngineCashInEventReader> _logger;
        private readonly IMatchingEngineSubscriber<CashInEvent>[] _subscribers;

        public MatchingEngineCashInEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<CashInEvent>[] subscribers,
            ILogger<MatchingEngineCashInEventReader> logger) 
            : base(settings.RabbitMqConnectionString, settings.PrefetchCount, settings.BatchCount, logger)
        {
            _logger = logger;
            _subscribers = subscribers;
            ExchangeName = settings.TopicName;
            QueueName = settings.QueryName;
        }

        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override string[] RoutingKeys { get; } = { MessageType.CashIn.ToString() };

        protected override async Task ProcessBatch(IList<CustomQueueItem<CashInEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle CashInEvent's'");
                    throw;
                } 
            }
        }

        protected override void LogQueue()
        {
        }

        protected override CashInEvent DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            var item = CashInEvent.Parser.ParseFrom(body.ToArray());
            return item;
        }
    }
}