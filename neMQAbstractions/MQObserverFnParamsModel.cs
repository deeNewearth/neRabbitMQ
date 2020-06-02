using System;
using System.Collections.Generic;
using System.Text;

namespace neMQConnector
{
    public class MQObserverFnParamsModel
    {
        public MQSubjectModel message { get; set; }
        public string routingKey { get; set; }
        public ulong deliveryTag { get; set; }
        public ulong messageCount { get; set; }

        public bool isRedelivered { get; set; }

        /// <summary>
        /// Get the mesage that is to be sent over the wire
        /// </summary>
        /// <returns></returns>
        public object[] getWireMessage()
        {
            return new object[] { this.message.subjectId, this.message.ownerId, this.message.reason ?? "", this.routingKey, this.deliveryTag, this.messageCount };
        }
    }
}
