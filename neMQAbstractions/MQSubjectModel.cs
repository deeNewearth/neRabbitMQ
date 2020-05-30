using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;

namespace neMQConnector
{
    [JsonConverter(typeof(MQSubjectModelConverter))]
    public class MQSubjectModel
    {
        /// <summary>
        /// The id of the message subject example order
        /// </summary>
        public string subjectId { get; set; }

        /// <summary>
        /// owner of the message subject object
        /// </summary>
        public string ownerId { get; set; }

        /// <summary>
        /// Why was this message sent
        /// for example OrderTriggersModel trigger
        /// </summary>
        public string reason { get; set; }

        /// <summary>
        /// The actual derived class
        /// </summary>
        [ExportAsOptional]
        public string qualifiedName => this.GetType().AssemblyQualifiedName;

        /// <summary>
        /// contains internal data for a consumer 
        /// </summary>
        public Dictionary<Guid, ConnectorInfoModel> connectorInstanceData { get; set; }

        public T convertedTo<T>() where T: MQSubjectModel
        {
            var requiredType = typeof(T);
            var actualType = this.GetType();

            if (!(requiredType == actualType || actualType.IsSubclassOf(requiredType)))
                throw new InvalidOperationException($"cannot get {requiredType} from {actualType}");

            return this as T;
        }

        public MQSubjectModel(string _subjetId)
            :this()
        {
            this.subjectId = _subjetId;
        }


        [JsonConstructor]
        private MQSubjectModel()
        {
            connectorInstanceData = new Dictionary<Guid, ConnectorInfoModel>();
        }
    }


    public class MQSubjectModelConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
            => typeof(MQSubjectModel) == objectType || objectType.IsSubclassOf(typeof(MQSubjectModel));

        public override bool CanRead => true;


        // implement this if you need to read the string representation to create an AccountId
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var token = JToken.Load(reader);
            var typeToken = token["qualifiedName"];
            if (typeToken == null)
                throw new InvalidOperationException("invalid object");

            var actualType = Type.GetType(typeToken.ToString());

            if (!this.CanConvert(actualType))
                throw new InvalidOperationException("invalid object base class");

            if (existingValue == null || existingValue.GetType() != actualType)
            {
                var contract = serializer.ContractResolver.ResolveContract(actualType);
                existingValue = contract.DefaultCreator();
            }
            using (var subReader = token.CreateReader())
            {
                // Using "populate" avoids infinite recursion.
                serializer.Populate(subReader, existingValue);
            }
            return existingValue;
        }


        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!(value is MQSubjectModel))
                throw new JsonSerializationException("Expected MQSubjectModel object value.");

            if (!this.CanConvert(value.GetType()))
            {
                return;
            }

            var entity = value as MQSubjectModel;
            if (entity == null) return;

            writer.WriteStartObject();
            var props = entity.GetType().GetProperties();
            foreach (var propertyInfo in props)
            {
                var tempVal = propertyInfo.GetValue(entity);
                if (tempVal == null) continue;

                writer.WritePropertyName(propertyInfo.Name);
                serializer.Serialize(writer, tempVal);
            }

            writer.WriteEndObject();
        }
    }
}
