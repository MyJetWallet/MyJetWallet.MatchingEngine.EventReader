using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ME.Contracts.OutgoingMessages;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineGlobalEventReader : BaseBatchQueueReader<OutgoingEvent>
    {
        private readonly ILogger<MatchingEngineGlobalEventReader> _logger;
        private readonly IMatchingEngineSubscriber<OutgoingEvent>[] _subscribers;

        public MatchingEngineGlobalEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<OutgoingEvent>[] subscribers,
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

        protected override async Task ProcessBatch(IList<CustomQueueItem<OutgoingEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle OutgoingEvent's");
                    throw;
                }
            }
        }

        protected override void LogQueue()
        {
        }

        protected override OutgoingEvent DeserializeMessage(ReadOnlyMemory<byte> body, string routingKey)
        {
            try
            {
                var item = OutgoingEvent.Parser.ParseFrom(body.ToArray());
                return item;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Cannot parse ME OutgoingEvent. routingKey: {routingKey}. Message base64: {Convert.ToBase64String(body.ToArray())}.");
                Console.WriteLine($"Cannot parse ME OutgoingEvent. routingKey: {routingKey}. Message base64: {Convert.ToBase64String(body.ToArray())}.");
                throw;
            }
        }
    }
}