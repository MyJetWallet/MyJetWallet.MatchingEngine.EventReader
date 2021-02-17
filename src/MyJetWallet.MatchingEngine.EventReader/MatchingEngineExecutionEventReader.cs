using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchinEngine.Events.Models;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineExecutionEventReader : BaseBatchQueueReader<ExecutionEvent>
    {
        private readonly ILogger<MatchingEngineExecutionEventReader> _logger;
        private readonly IMatchingEngineSubscriber<ExecutionEvent>[] _subscribers;

        public MatchingEngineExecutionEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<ExecutionEvent>[] subscribers,
            ILogger<MatchingEngineExecutionEventReader> logger)
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
        protected override string[] RoutingKeys { get; } = { ((int)MessageType.Order).ToString() };

        protected override async Task ProcessBatch(IList<CustomQueueItem<ExecutionEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle ExecutionEvent's'");
                    throw;
                }
            }
        }

        protected override void LogQueue()
        {
        }

        protected override ExecutionEvent DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            var item = ExecutionEvent.Parser.ParseFrom(body.ToArray());
            return item;
        }
    }
}