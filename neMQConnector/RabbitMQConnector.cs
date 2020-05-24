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

        Task publishAsync(string routingKey, MQSubjectModel message);

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
    }

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
    public class RabbitMQConnector : IRabbitMQConnector
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
                _myConfig = _configuration.GetValue<MyConfig>("rabbitMQ");
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


        /*
        public Task<MQObserverIdModel> AddMQPullerAsync(string name, string routingKey)
        {
            return AddObserverAsync(new MQObserverIdModel
            {
                name = name,
                routingKey = routingKey,
                pullEnabled = true
            });
        }
        */

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
            _publishChannel = null;

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

                startPublisher();

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

        void startWorker(KeyValuePair<MQObserverIdModel, ConsumerChannel> observer)
        {
            _logger.LogInformation($"Staring observer {observer.Key}");
            var consumeChannel = _amqpConn.CreateModel();

            var properties = consumeChannel.CreateBasicProperties();
            properties.Persistent = true;

            consumeChannel.BasicQos(prefetchSize: 0, prefetchCount: observer.Key.preFetchCount, global: false);



            var qDeclrae = consumeChannel.QueueDeclare(queue: observer.Key.qName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

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
                        try
                        {
                            var message = Encoding.UTF8.GetString(ea.Body);
                            _logger.LogDebug($"RabbitMQ message {ea.DeliveryTag} Received '{ea.RoutingKey}':'{message}'");

                            MQSubjectModel msgSubject;


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
                            _logger.LogWarning(ex, $"exception {ea.DeliveryTag}");
                            await Task.Delay(TimeSpan.FromSeconds(5));
                            consumeChannel.BasicReject(ea.DeliveryTag, true);
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

        IModel _publishChannel = null;
        void startPublisher()
        {
            try
            {
                _publishChannel = _amqpConn.CreateModel();

                _publishChannel.ModelShutdown += (o, e) =>
                {
                    _logger.LogWarning($"ModelShutdown : {e}");
                };


                _publishChannel.CallbackException += (o, e) =>
                {
                    _logger.LogWarning(e.Exception, $"CallbackException");
                };

                _publishChannel.ExchangeDeclare(exchange: _myConfig.exchange, type: "topic", durable: true);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to open RabbitMQ channel");
                _amqpConn.Close();
            }

        }

        public async Task publishAsync(string routingKey, MQSubjectModel message)
        {
            try
            {
                var properties = _publishChannel.CreateBasicProperties();
                properties.Persistent = true;

                var msgStr = JsonConvert.SerializeObject(message);

                _publishChannel.BasicPublish(exchange: _myConfig.exchange,
                     routingKey: routingKey,
                     basicProperties: properties,
                     body: Encoding.UTF8.GetBytes(msgStr));

                //_publishChannel.WaitForConfirmsOrDie();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "failed to publish");
                await Task.Delay(TimeSpan.FromSeconds(5));
                await publishAsync(routingKey, message);
            }

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