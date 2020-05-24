using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace neMQConnector
{
    /// <summary>
    /// Cannot figure out how to use IHostLife line etc and all to shut down application  so using my own
    /// </summary>
    public interface IHackedAppLifeline
    {
        void Shutdown();
    }

    public class SimpleLifeTime : IHackedAppLifeline
    {
        readonly CancellationTokenSource _stopSource = new CancellationTokenSource();

        public CancellationToken stopToken { get { return _stopSource.Token; } }

        public void Shutdown()
        {
            _stopSource.Cancel();
       
        }
       
    }
}
