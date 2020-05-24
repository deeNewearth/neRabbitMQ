using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace neMQConnector
{
    public class MQObserverIdModel
    {
        /// <summary>
        /// OPTIONAL: SignalIR connectionID  Used to connect more then 1 consumer to a Q
        /// </summary>
        [ExportAsOptional]
        public string consumerId { get; set; }

        /// <summary>
        /// The application userId that made this connection
        /// </summary>
        [ExportAsOptional]
        public string userId { get; set; }

        /// <summary>
        /// Q identifier
        /// </summary>
        public string name { get; set; }

        public string routingKey { get; set; }

        //this is set to TRU for Q where we want manual ack
        [ExportAsOptional]
        public bool pullEnabled { get; set; }


        //do we want to Ack this at a later point
        [ExportAsOptional]
        public bool manualAck { get; set; }

        [ExportAsOptional]
        public ushort preFetchCount { get; set; }

        /// <summary>
        /// Q name in rabbit MQ
        /// </summary>
        [JsonIgnore]
        public string qName
        {
            get
            {
                var ret = $"{name}_{routingKey}_{preFetchCount}";

                if (pullEnabled)
                {
                    ret += "_pullOnly";
                }

                if (manualAck)
                {
                    ret += "_manualAck";
                }

                return ret;
            }
        }

        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
                return false;

            var b2 = (MQObserverIdModel)obj;

            return qName == b2.qName && consumerId == b2.consumerId && userId == b2.userId;

            //This way we don't have to change this eveytime we add or remove members
            //return JsonConvert.SerializeObject(this) == JsonConvert.SerializeObject(b2);

            //return (name == b2.name && routingKey == b2.routingKey && pullEnabled == b2.pullEnabled);
        }

        public override int GetHashCode()
        {
            return qName.GetHashCode() ^ (consumerId ?? "").GetHashCode() ^ (userId ?? "").GetHashCode();

            //return name.GetHashCode() ^ routingKey.GetHashCode() ^ pullEnabled.GetHashCode();
        }

        public static bool operator ==(MQObserverIdModel b1, MQObserverIdModel b2)
        {
            if ((object)b1 == null)
                return (object)b2 == null;

            return b1.Equals(b2);
        }

        public static bool operator !=(MQObserverIdModel b1, MQObserverIdModel b2)
        {
            return !(b1 == b2);
        }


        /*
        /// <summary>
        /// Used to unsubscrive previous manual ACK connections when the same user tries to to connect again
        /// </summary>
        /// <param name="consumerId"></param>
        /// <returns></returns>
        public MQObserverIdModel getPreviousId(string consumerId)
        {
            var t = this.MemberwiseClone() as MQObserverIdModel;
            t.consumerId = consumerId;
            return t;
        }
        */

    }
}