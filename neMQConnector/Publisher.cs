using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace neMQConnector
{
    public partial class RabbitMQConnector
    {
        struct MessageToPublis
        {
            public string routingKey;
            public MQSubjectModel message;
            public byte priority;
            public Action<Exception> onPublished;
            public TimeSpan timeOut;
        }

        //This can only be accessed from the Thread it was created So

        readonly BlockingCollection<MessageToPublis> commandsForDedicatedThread = new BlockingCollection<MessageToPublis>();
        

        /// <summary>
        /// Dedcated Thread for publishChannel. Created and started in the constructor
        /// </summary>
        Thread dedicatedThreadForReaderWriterLockSlim = null;
        CancellationTokenSource _cts;

        void ThreadFunction()
        {
            IModel _publishChannel = null;
            try
            {
                _cts = new CancellationTokenSource();
                while (true)
                {
                    if (_cts.Token.IsCancellationRequested)
                    {
                        break;
                    }

                    MessageToPublis toPublish;
                    if (commandsForDedicatedThread.TryTake(out toPublish, (int)TimeSpan.FromMinutes(5).TotalMilliseconds, _cts.Token))
                    {
                        try
                        {

                            if (null == _publishChannel)
                            {
                                _publishChannel = _amqpConn.CreateModel();
                                _publishChannel.ConfirmSelect();

                                _publishChannel.ModelShutdown += (o, e) =>
                                {
                                    _logger.LogWarning($"ModelShutdown : {e}");
                                    _cts.Cancel();
                                };

                                _publishChannel.CallbackException += (o, e) =>
                                {
                                    _logger.LogWarning(e.Exception, $"CallbackException");
                                };

                                _publishChannel.ExchangeDeclare(exchange: _myConfig.exchange, type: "topic", durable: true);

                            }


                            var properties = _publishChannel.CreateBasicProperties();
                            properties.Persistent = true;

                            if (toPublish.priority != 0)
                            {
                                properties.Priority = toPublish.priority;
                            }

                            var msgStr = JsonConvert.SerializeObject(toPublish.message);

                            _publishChannel.BasicPublish(exchange: _myConfig.exchange,
                                 routingKey: toPublish.routingKey,
                                 basicProperties: properties,
                                 body: Encoding.UTF8.GetBytes(msgStr));

                            _publishChannel.WaitForConfirmsOrDie(toPublish.timeOut);

                            toPublish.onPublished(null);

                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to publish to MQ. Terminating this channel");
                            _cts.Cancel();
                            toPublish.onPublished(ex);
                            break;
                        }
                    }
                    else
                    {
                        _logger.LogDebug("tryTake timed out will continue");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to open RabbitMQ channel");
            }
            finally
            {
                dedicatedThreadForReaderWriterLockSlim = null;
                _cts = null;
            }
        }

        public async Task publishAsync(string routingKey, MQSubjectModel message, byte priority = 0, TimeSpan? timeOut = null)
        {
            if (null == dedicatedThreadForReaderWriterLockSlim)
            {
                var originalThread = Interlocked.CompareExchange(ref dedicatedThreadForReaderWriterLockSlim, new Thread(ThreadFunction), null);
                if (null == originalThread)
                {
                    _logger.LogInformation("Pblisher thread created");
                    dedicatedThreadForReaderWriterLockSlim.Start();
                }
            }

            Exception publisException = null;
            CancellationTokenSource publishWaiter = new CancellationTokenSource();

            commandsForDedicatedThread.Add(new MessageToPublis
            {
                message = message,
                priority = priority,
                routingKey = routingKey,
                timeOut = timeOut ?? TimeSpan.FromSeconds(15),
                onPublished = ex =>
                {
                    publisException = ex;
                    publishWaiter.Cancel();
                }
            });

            try
            {
                //prettu
                await Task.Delay(-1, publishWaiter.Token);
            }
            catch (Exception) { }

            if (null != publisException)
                throw publisException;
        }

    }
}
