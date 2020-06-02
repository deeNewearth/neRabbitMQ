using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;



namespace neMQConnector
{
    

    
    

    /// <summary>
    /// The MQ connector SINGLETON
    /// 
    /// 
    /// services.AddSingleton<neMQConnector.IHackedAppLifeline, neMQConnector.SimpleLifeTime>();
    /// 
    /// services.AddSingleton<neMQConnector.IRabbitMQConnector, neMQConnector.RabbitMQConnector>();
    /// services.AddSingleton<IHostedService, neMQConnector.RabbitMQService>();
    /// 
    /// </summary>
    public partial class RabbitMQConnector : IRabbitMQConnector
    {
        readonly MyConfig _myConfig;
        readonly ILogger _logger;
        readonly IConfiguration _configuration;
        readonly IHackedAppLifeline _appLifetime;
        readonly ConnectionFactory _factory = null;


        readonly TaskScheduler _ObserverScheduler;

        public RabbitMQConnector(
            IHackedAppLifeline appLifetime,
            ILogger<RabbitMQConnector> logger,
            IConfiguration configuration
            )
        {

            _configuration = configuration;
            _logger = logger;
            _appLifetime = appLifetime;

            try
            {
                _myConfig = _configuration.GetSection("rabbitMQ").Get<MyConfig>();
                if (null == _myConfig)
                    throw new Exception("config section rabbitMQ not found");

                if (string.IsNullOrWhiteSpace(_myConfig.exchange))
                    throw new Exception("RabbitMQ exchange is not configured");


                if (string.IsNullOrWhiteSpace(_myConfig.hostname))
                    throw new Exception("RabbitMQ hostname is not configured");


                if (string.IsNullOrWhiteSpace(_myConfig.user))
                    throw new Exception("RabbitMQ user is not configured");


                if (string.IsNullOrWhiteSpace(_myConfig.pass))
                    throw new Exception("RabbitMQ pass is not configured");


                _ObserverScheduler = new ConcurrentExclusiveSchedulerPair(
                    TaskScheduler.Default,          // schedule work to the ThreadPool
                    _myConfig.maxTasks).ConcurrentScheduler;


                _logger.LogInformation($"RabbitMQ connection: {_myConfig.hostname}:{_myConfig.user}:{_myConfig.pass}. Max Tasks:{_myConfig.maxTasks}");

                _factory = new ConnectionFactory()
                {
                    HostName = _myConfig.hostname,
                    UserName = _myConfig.user,
                    Password = _myConfig.pass,
                    AutomaticRecoveryEnabled = true,
                };
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "failed to create RabbitMQ shutting down");
                _appLifetime.Shutdown();
                throw ex;
            }

        }

        volatile bool _isStopping = false;
        public void Teardown()
        {
            _isStopping = true;
            if (null != _amqpConn)
                _amqpConn.Close();
        }

        class ConsumerChannel
        {
            public IModel channel { get; set; }

            /// <summary>
            /// The call back method to be invoked for Subscribed channels
            /// </summary>
            public IMqCallback callbackObject { get; set; }
        }

        readonly ConcurrentDictionary<MQObserverIdModel, ConsumerChannel> _observers = new ConcurrentDictionary<MQObserverIdModel, ConsumerChannel>();

        public async Task AddObserverAsync(string name, string routingKey, MQObserverFn actionAsync, bool manualAck = false, ushort preFetchCount = 1)
        {
            await AddObserverAsync(new MQObserverIdModel
            {
                name = name,
                routingKey = routingKey,
                pullEnabled = false,
                manualAck = manualAck,
                preFetchCount = preFetchCount
            }, actionAsync);
        }


        
        public IReadOnlyDictionary<MQObserverIdModel, IMqCallback> currentObservers { get { return _observers.ToDictionary(k => k.Key, v => v.Value.callbackObject); } }

        public void Ack(MQObserverIdModel observerId, ulong deliveryTag)
        {
            if (!_observers.ContainsKey(observerId))
                return;

            var observerFn = _observers[observerId];

            if (null == observerFn.channel)
            {
                _logger.LogWarning($"channel for {observerId.name}-{observerId.routingKey} is null");
                return;
            }

            observerFn.channel.BasicAck(deliveryTag, false);
        }

        /*
        public IEnumerable<MQObserverFnParamsModel> GetMessages(MQObserverIdModel observerId)
        {
            //not using it right now take this out
            throw new NotImplementedException();

            if (!observerId.pullEnabled)
                throw new InvalidOperationException("this Q is for subscription");

            var observerFn = _observers[observerId];

            if(null == observerFn.channel)
            {
                _logger.LogWarning($"channel for {observerId.name}-{observerId.routingKey} is null");
                yield break;
            }

            BasicGetResult ea;
            while(null != (ea = observerFn.channel.BasicGet(observerId.qName, false)))
            {
                yield return new MQObserverFnParamsModel
                {
                    deliveryTag = ea.DeliveryTag,
                    message = JsonConvert.DeserializeObject< MQSubjectModel >( Encoding.UTF8.GetString(ea.Body)),
                    routingKey = ea.RoutingKey,
                    messageCount = observerFn.channel.MessageCount(observerId.qName)
                };
            }
        }
        */

        public void RemoveObserver(MQObserverIdModel observerId)
        {
            ConsumerChannel consumer;
            if (_observers.TryRemove(observerId, out consumer))
            {
                consumer.channel.Close();
            }
            else
            {
                _logger.LogDebug($"remove received for non existing key {observerId.qName}");
                return;
            }
        }

        class CBObject : IMqCallback
        {
            public MQObserverFn callback { get; set; }
        }

        public Task<IMqCallback> AddObserverAsync(MQObserverIdModel observerId, MQObserverFn actionAsync)
        {
            return AddObserverAsync(observerId, new CBObject { callback = actionAsync });
        }


        public async Task<IMqCallback> AddObserverAsync(MQObserverIdModel observerId, IMqCallback cbObject)
        {
            _logger.LogInformation($"Adding observer {observerId.qName}");

            var newChannel = new ConsumerChannel { callbackObject = cbObject };

            if (!_Servicestarted)
            {
                _observers[observerId] = newChannel;
                return cbObject;
            }

            if (_observers.ContainsKey(observerId))
            {
                _observers[observerId] = newChannel;
                _logger.LogInformation($"observer {observerId.name}:{observerId.routingKey} exists. Restarting connection");
                await StartAsync();
            }
            else
            {
                _observers[observerId] = newChannel;
                startWorker(_observers.Single(k => k.Key == observerId));
            }

            return cbObject;

        }

        volatile int _recoveryRetryCount = 0;

        IConnection _amqpConn = null;

        volatile bool _Servicestarted = false;

        public async Task StartAsync()
        {
            var isRestart = null != _amqpConn;

            _Servicestarted = false;
            _amqpConn = null;
            

            foreach (var c in _observers)
                c.Value.channel = null;


            if (_isStopping)
                return;

            //we wait a second before starting.
            if (isRestart)
                await Task.Delay(TimeSpan.FromSeconds(10));

            if (_recoveryRetryCount > 50)
            {
                _appLifetime.Shutdown();
                throw new Exception("cannot connect to mq");
            }

            try
            {
                _amqpConn = _factory.CreateConnection();

                _amqpConn.ConnectionShutdown += (o, e) =>
                {
                    _logger.LogInformation($"shutdown : {e}");
                    Task.Run(() => StartAsync());

                };

                _recoveryRetryCount = 0;

                _amqpConn.ConnectionRecoveryError += (o, e) =>
                {
                    _logger.LogCritical($"failed to recover: {e}");

                    if (_recoveryRetryCount > 20)
                    {
                        _logger.LogCritical("Cannot connect to MQ shutting down");
                        _appLifetime.Shutdown();
                    }


                    _recoveryRetryCount++;
                };

                _logger.LogInformation("connection established");


                foreach (var c in _observers)
                    startWorker(c);

                _Servicestarted = true;

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to connect to RabbitMQ");
                await Task.Delay(TimeSpan.FromSeconds(10));
                _recoveryRetryCount++;
                await StartAsync();
                return;
            }

        }

        /// <summary>
        /// Thrown by observers to NOT reQ the message
        /// </summary>
        public class QNoRetryException : Exception
        {
            public QNoRetryException(string message, Exception inner = null) : base(message, inner) { }
        }

        /// <summary>
        /// we want this to be reset when we reboot this service
        /// </summary>
        static readonly Guid _serviceInstanceId = Guid.NewGuid();

        public Guid currentInstanceId { get => _serviceInstanceId; }

        void startWorker(KeyValuePair<MQObserverIdModel, ConsumerChannel> observer)
        {
            _logger.LogInformation($"Staring observer {observer.Key}");
            var consumeChannel = _amqpConn.CreateModel();

            var properties = consumeChannel.CreateBasicProperties();
            properties.Persistent = true;

            consumeChannel.BasicQos(prefetchSize: 0, prefetchCount: observer.Key.preFetchCount, global: false);

            Dictionary<string, object> qArgs = null;
            if(observer.Key.maxPriority > 0)
            {
                qArgs = new Dictionary<string, object>();
                qArgs["x-max-priority"] = observer.Key.maxPriority;
            }

            var qDeclrae = consumeChannel.QueueDeclare(queue: observer.Key.qName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: qArgs);

            consumeChannel.QueueBind(queue: observer.Key.qName,
                                      exchange: _myConfig.exchange,
                                      routingKey: observer.Key.routingKey,
                                      arguments: null);

            if (observer.Key.pullEnabled)
            {
                //nothing to do for pull NOW
            }
            else
            {
                var consumer = new EventingBasicConsumer(consumeChannel);
                consumer.Received += (model, ea) =>
                {
                    Task.Factory.StartNew(async () =>
                    {
                        MQSubjectModel msgSubject =null;
                        byte msgPriority = 0;
                        if(null != ea.BasicProperties && ea.BasicProperties.Priority > 0)
                        {
                            msgPriority = ea.BasicProperties.Priority;
                        }
                        try
                        {


                            var message = Encoding.UTF8.GetString(ea.Body);
                            _logger.LogDebug($"RabbitMQ message {ea.DeliveryTag} Received '{ea.RoutingKey}':'{message}'");

                            try
                            {
                                msgSubject = JsonConvert.DeserializeObject<MQSubjectModel>(message);
                                if (null == msgSubject)
                                    throw new Exception("null message, cannot send it");
                            }
                            catch (Exception ex)
                            {
                                Debug.Assert(false);
                                throw new QNoRetryException($"MQSubjectModel cannot be deserialized :{message} ", inner: ex);
                            }

                            
                            var dataToSend = new MQObserverFnParamsModel
                            {
                                isRedelivered = ea.Redelivered,
                                deliveryTag = ea.DeliveryTag,
                                message = msgSubject,
                                routingKey = ea.RoutingKey,
                                //messageCount = consumeChannel.MessageCount(observer.Key.qName)
                                messageCount = qDeclrae.MessageCount
                            };

                            await observer.Value.callbackObject.callback(dataToSend);

                            _logger.LogDebug($"RabbitMQ message {ea.DeliveryTag} Completed ");

                            if (!observer.Key.manualAck)
                            {
                                consumeChannel.BasicAck(ea.DeliveryTag, false);
                            }
                        }
                        catch (QNoRetryException ex)
                        {
                            _logger.LogDebug(ex, $"no retry exception {ea.DeliveryTag}");
                            consumeChannel.BasicReject(ea.DeliveryTag, false);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, $"Exception while processing message {ea.DeliveryTag}");

                            if(null == msgSubject)
                            {
                                _logger.LogCritical(ex,"We should not be here is we failed to serialize the message");
                                consumeChannel.BasicReject(ea.DeliveryTag, false);
                            }

                            /* DEE seems like we cannot guarentee that the message will be published
                             * NO idea why manuall re -publishing just doesnt work. The message gets lost 
                            if(null == msgSubject.connectorInstanceData)
                            {
                                _logger.LogDebug("this message does not uses advance requing");
                                consumeChannel.BasicReject(ea.DeliveryTag, true);
                                return;
                            }

                            if (!msgSubject.connectorInstanceData.ContainsKey(_serviceInstanceId))
                            {
                                msgSubject.connectorInstanceData[_serviceInstanceId] = new ConnectorInfoModel();
                            }

                            var retryCount = ++msgSubject.connectorInstanceData[_serviceInstanceId].retryCount;
                            if (retryCount > 5)
                            {
                                var delaySeconds = 5 * (retryCount - 5);
                                _logger.LogDebug($"message {ea.DeliveryTag} has failed {retryCount} times. We will delay by {delaySeconds} seconds");
                                await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                            }
                            else
                            {
                                _logger.LogDebug($"message {ea.DeliveryTag} has failed {retryCount} times.");
                            }

                            
                            
                            await this.publishAsync(ea.RoutingKey, msgSubject, msgPriority);

                            consumeChannel.BasicReject(ea.DeliveryTag, false);
                            */

                            consumeChannel.BasicReject(ea.DeliveryTag, true);

                            //dee todo: Dead Exchange logic here

                        }
                    },CancellationToken.None,TaskCreationOptions.None,_ObserverScheduler);

                };

                consumeChannel.BasicConsume(queue: observer.Key.qName,
                                                 autoAck: false,
                                                 consumer: consumer,
                                                 arguments: null, consumerTag: String.Empty, exclusive: false, noLocal: false
                                                 );
            }

            //We store the channel in the the observer map
            observer.Value.channel = consumeChannel;
        }

        

    }

    /// <summary>
    /// Used by asp core to start this service
    /// </summary>
    public class RabbitMQService : Microsoft.Extensions.Hosting.IHostedService
    {
        readonly IRabbitMQConnector _connector;
        public RabbitMQService(IRabbitMQConnector connector)
        {
            _connector = connector;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return _connector.StartAsync();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _connector.Teardown();

            return Task.FromResult(true);
        }
        
    }
}