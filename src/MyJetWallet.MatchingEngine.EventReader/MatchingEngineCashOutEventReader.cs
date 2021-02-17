﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchinEngine.Events.Models;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineCashOutEventReader : BaseBatchQueueReader<CashOutEvent>
    {
        private readonly ILogger<MatchingEngineCashOutEventReader> _logger;
        private readonly IMatchingEngineSubscriber<CashOutEvent>[] _subscribers;

        public MatchingEngineCashOutEventReader(
            MatchingEngineEventReaderSettings settings,
            IMatchingEngineSubscriber<CashOutEvent>[] subscribers,
            ILogger<MatchingEngineCashOutEventReader> logger)
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
        protected override string[] RoutingKeys { get; } = { MessageType.CashOut.ToString() };

        protected override async Task ProcessBatch(IList<CustomQueueItem<CashOutEvent>> batch)
        {
            foreach (var subs in _subscribers)
            {
                try
                {
                    await subs.Process(batch);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Subscriber {subs.GetType().Name} cannot handle CashOutEvent's'");
                    throw;
                }
            }
        }

        protected override void LogQueue()
        {
        }

        protected override CashOutEvent DeserializeMessage(ReadOnlyMemory<byte> body)
        {
            var item = CashOutEvent.Parser.ParseFrom(body.ToArray());
            return item;
        }
    }
}