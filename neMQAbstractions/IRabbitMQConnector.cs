using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace neMQConnector
{
    public delegate Task MQObserverFn(MQObserverFnParamsModel e);

    public interface IMqCallback
    {
        MQObserverFn callback { get; }
    }
    /// <summary>
    /// We only put a identfier ID in rabbit MQ. 
    /// The idea is to let an observer know that it has something in the Queue.
    /// it is possible that by the time the observer wants to process the task. It is no longer required
    /// Observers should look at other status fields in mongo to find out if the task is still relevent
    /// </summary>
    public interface IRabbitMQConnector
    {
        Task StartAsync();
        void Teardown();

        Task publishAsync(string routingKey, MQSubjectModel message, byte priority = 0, TimeSpan? timeOut = null);

        /// <summary>
        /// Subscribe to a message Q
        /// </summary>
        /// <param name="name">User friendly name of the subscription. Doesn't need to be unique. This is useful in identifying in the admin view</param>
        /// <param name="routingKey"></param>
        /// <param name="actionAsync"></param>
        /// <param name="manualAck"></param>
        /// <param name="preFetchCount"></param>
        /// <returns></returns>
        Task AddObserverAsync(string name, string routingKey, MQObserverFn actionAsync, bool manualAck = false, ushort preFetchCount = 1);

        Task<IMqCallback> AddObserverAsync(MQObserverIdModel observerId, MQObserverFn actionAsync);

        Task<IMqCallback> AddObserverAsync(MQObserverIdModel observerId, IMqCallback cbObject);

        //Creates a channel to be used for pull and Manual Ack
        /*   Task<MQObserverIdModel> AddMQPullerAsync(string name, string routingKey);
        IEnumerable<MQObserverFnParamsModel> GetMessages(MQObserverIdModel observerId);
        */

        void Ack(MQObserverIdModel observerId, ulong deliveryTag);

        void RemoveObserver(MQObserverIdModel observerId);

        IReadOnlyDictionary<MQObserverIdModel, IMqCallback> currentObservers { get; }

        Guid currentInstanceId { get; }
    }
}
