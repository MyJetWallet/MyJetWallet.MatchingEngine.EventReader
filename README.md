**nuget packege:** ![Nuget version](https://img.shields.io/nuget/v/MyJetWallet.MatchingEngine.EventReader?label=MyJetWallet.MatchingEngine.EventReader&style=social)

## Usage

```csharp

public class CashInEventHandler: IMatchingEngineSubscriber<CashInEvent>
{
    public Task Process(IList<CustomQueueItem<CashInEvent>> batch)
    {
        return Task.CompletedTask;
    }
}

var settings = new MatchingEngineEventReaderSettings("amqp://user:pass@rabbit:5672", "my-queue-name");

var handler = new CashInEventHandler();

var reader = new MatchingEngineCashInEventReader(settings, new [] {handler}, logfactory.CreateLog<MatchingEngineCashInEventReader>());
reader.Start();

.....


reader.Stop();

```
