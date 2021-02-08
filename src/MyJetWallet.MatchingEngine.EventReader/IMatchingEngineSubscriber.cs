using System.Collections.Generic;
using System.Threading.Tasks;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;

namespace MyJetWallet.MatchingEngine.EventReader
{
    public interface IMatchingEngineSubscriber<T>
    {
        Task Process(IList<CustomQueueItem<T>> batch);
    }
}