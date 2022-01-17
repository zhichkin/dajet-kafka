using Confluent.Kafka;
using DaJet.Data.Messaging;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace DaJet.Kafka
{
    public sealed class KafkaMessageProducer
    {
        private readonly string _topic;
        private readonly string _server;
        private readonly string _client;
        private readonly ProducerConfig _config;
        private readonly Message<Null, string> _message = new Message<Null, string>();

        public KafkaMessageProducer(in string server, in string topic, in string client)
        {
            _topic = topic;
            _server = server;
            _client = client;

            _config = new ProducerConfig()
            {
                ClientId = _client,
                BootstrapServers = _server,
                Acks = Acks.All,
                MaxInFlight = 1,
                MessageTimeoutMs = 5000,
                EnableIdempotence = false
            };
        }
        public int Produce(in IMessageConsumer consumer)
        {
            int produced = 0;

            using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(_config).Build())
            {
                do
                {
                    consumer.TxBegin();

                    foreach (OutgoingMessage message in consumer.Select())
                    {
                        SetMessageHeaders(in message);

                        _message.Value = message.MessageBody;

                        producer.Produce(_topic, _message);
                    }

                    producer.Flush();
                    consumer.TxCommit();

                    produced += consumer.RecordsAffected;
                }
                while (consumer.RecordsAffected > 0);
            }

            return produced;
        }
        private void SetMessageHeaders(in OutgoingMessage message)
        {
            _message.Headers = null;

            if (string.IsNullOrWhiteSpace(message.Headers))
            {
                return;
            }

            Dictionary<string, string> headers = JsonSerializer.Deserialize<Dictionary<string, string>>(message.Headers);

            _message.Headers = new Headers();

            foreach (var header in headers)
            {
                _message.Headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
            }
        }
    }
}