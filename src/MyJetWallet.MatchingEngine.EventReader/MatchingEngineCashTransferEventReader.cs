using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchinEngine.Events.Models;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineCashTransferEventReader : BaseBatchQueueReader<CashTransferEvent>
    {
        private readonly ILogger<MatchingEngineCashTransferEventReader> _logger;
        private readonly IMatchingEngineSubscriber<CashTransferEvent>[] _subscribers;

        public MatchingEngineCashTransferEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<CashTransferEvent>[] subscribers,
            ILogger<MatchingEngineCashTransferEventReader> logger)
            : base(settings.RabbitMqConnectionString, settings.PrefetchCount, settings.BatchCount, logger)
        {
            _logger = logger;
            _subscribers = subscribers;
            ExchangeName = settings.TopicName;
            QueueName = settings.QueryName;
        }

        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override string[] RoutingKeys { get; } = { MessageType.CashTransfer.ToString() };

        protected override async Task ProcessBatch(IList<CustomQueueItem<CashTransferEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle CashTransferEvent's'");
                    throw;
                }
            }
        }

        protected override void LogQueue()
        {
        }

        protected override CashTransferEvent DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            var item = CashTransferEvent.Parser.ParseFrom(body.ToArray());
            return item;
        }
    }
}