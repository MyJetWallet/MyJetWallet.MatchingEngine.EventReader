using System.Collections.Generic;
using ME.Contracts.OutgoingMessages;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public class MatchingEngineEventReaderSettings
    {
        public MatchingEngineEventReaderSettings(string rabbitMqConnectionString, string queryName)
        {
            RabbitMqConnectionString = rabbitMqConnectionString;
            QueryName = queryName;
        }

        public string RabbitMqConnectionString { get; set; }
        public string QueryName { get; set; }

        public int PrefetchCount { get; set; } = 500;
        public int BatchCount { get; set; } = 100;
        public string TopicName { get; set; } = "ME";

        public bool IsQueueAutoDelete { get; set; } = false;
    }
}