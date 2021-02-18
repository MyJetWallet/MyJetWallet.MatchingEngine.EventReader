using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ME.Contracts.OutgoingMessages;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineGlobalEventReader : BaseBatchQueueReader<object>
    {
        private readonly ILogger<MatchingEngineGlobalEventReader> _logger;
        private readonly IMatchingEngineSubscriber<object>[] _subscribers;

        public MatchingEngineGlobalEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<object>[] subscribers,
            ILogger<MatchingEngineGlobalEventReader> logger)
            : base(settings.RabbitMqConnectionString, settings.PrefetchCount, settings.BatchCount, logger)
        {
            _logger = logger;
            _subscribers = subscribers;
            ExchangeName = settings.TopicName;
            QueueName = settings.QueryName;
            IsQueueAutoDelete = settings.IsQueueAutoDelete;

            RoutingKeys = settings.MessageTypes.Select(e => ((int) e).ToString()).ToArray();
        }

        protected override string ExchangeName { get; }
        protected override string QueueName { get; }
        protected override bool IsQueueAutoDelete { get; }
        protected override string[] RoutingKeys { get; }

        protected override async Task ProcessBatch(IList<CustomQueueItem<object>> batch)
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

        protected override object DeserializeMessage(ReadOnlyMemory<byte> body, string routingKey)
        {
            if (routingKey == ((int)Header.Types.MessageType.CashIn).ToString())
            {
                var item = CashInEvent.Parser.ParseFrom(body.ToArray());
                return item;
            }

            if (routingKey == ((int)Header.Types.MessageType.CashOut).ToString())
            {
                var item = CashOutEvent.Parser.ParseFrom(body.ToArray());
                return item;
            }

            if (routingKey == ((int)Header.Types.MessageType.CashTransfer).ToString())
            {
                var item = CashTransferEvent.Parser.ParseFrom(body.ToArray());
                return item;
            }

            if (routingKey == ((int)Header.Types.MessageType.Order).ToString())
            {
                var item = ExecutionEvent.Parser.ParseFrom(body.ToArray());
                return item;
            }

            Console.WriteLine($"Receive unknown event from ME: {routingKey}. Message will skipped");
            return null;
        }
    }
}