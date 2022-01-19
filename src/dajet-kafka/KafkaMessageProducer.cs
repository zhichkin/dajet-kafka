using Confluent.Kafka;
using DaJet.Data.Messaging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;

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

        private int _consumed;
        private int _produced;
        private string _error;
        private IMessageConsumer _consumer;

        public int Produce(in IMessageConsumer consumer)
        {
            int total = 0;
            _error = null;
            _consumer = consumer;
            
            using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(_config).Build())
            {
                do
                {
                    _consumer.TxBegin();

                    _consumed = 0;
                    _produced = 0;

                    foreach (OutgoingMessage message in consumer.Select())
                    {
                        SetMessageHeaders(in message);

                        _message.Value = message.MessageBody;

                        producer.Produce(_topic, _message, HandleDeliveryReport);
                    }
                    _consumed = _consumer.RecordsAffected;

                    if (_consumed > 0)
                    {
                        producer.Flush(); // wait for confirms

                        if (_consumed == _produced)
                        {
                            _consumer.TxCommit();
                            total += _consumed;
                        }
                        else
                        {
                            if (_error != null)
                            {
                                throw new Exception(_error);
                            }
                        }
                    }
                }
                while (_consumer.RecordsAffected > 0);
            }

            _error = null;
            _consumer = null;

            return total;
        }
        private void HandleDeliveryReport(DeliveryReport<Null, string> report)
        {
            if (report.Status == PersistenceStatus.Persisted)
            {
                Interlocked.Increment(ref _produced);
            }
            else
            {
                if (report.Error != null && string.IsNullOrWhiteSpace(_error))
                {
                    _error = report.Error.Reason;
                }
            }
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