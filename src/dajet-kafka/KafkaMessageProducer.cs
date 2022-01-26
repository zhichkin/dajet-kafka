using Confluent.Kafka;
using DaJet.Data.Messaging;
using DaJet.Logging;
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
                MessageTimeoutMs = 600000, // 600 seconds
                EnableIdempotence = false
            };

            _errorHandler = new Action<IProducer<Null, string>, Error>(ErrorHandler);
            _logHandler = new Action<IProducer<Null, string>, LogMessage>(LogHandler);
            _deliveryReportHandler = new Action<DeliveryReport<Null, string>>(HandleDeliveryReport);
        }

        private int _consumed;
        private int _produced;
        private string _error;
        private Action<IProducer<Null, string>, Error> _errorHandler;
        private Action<IProducer<Null, string>, LogMessage> _logHandler;
        private Action<DeliveryReport<Null, string>> _deliveryReportHandler;

        public int Produce(in IMessageConsumer consumer, in Dictionary<string, string> lookup)
        {
            int total = 0;
            _error = null;

            using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(_config)
                .SetLogHandler(_logHandler)
                .SetErrorHandler(_errorHandler)
                .Build())
            {
                do
                {
                    _produced = 0;

                    consumer.TxBegin();

                    foreach (OutgoingMessage message in consumer.Select())
                    {
                        SetMessageHeaders(in message);

                        _message.Value = message.MessageBody;

                        if (lookup.TryGetValue(message.MessageType, out string topic))
                        {
                            producer.Produce(topic, _message, _deliveryReportHandler);
                        }
                        else
                        {
                            producer.Produce(_topic, _message, _deliveryReportHandler);
                        }
                    }
                    _consumed = consumer.RecordsAffected;

                    producer.Flush();

                    if (_consumed == _produced)
                    {
                        consumer.TxCommit();
                        total += _consumed;
                    }
                    else
                    {
                        if (_error == null)
                        {
                            _error = "Producer error: _consumed != _produced";
                        }
                        throw new Exception(_error);
                    }
                }
                while (consumer.RecordsAffected > 0);
            }

            _error = null;

            return total;
        }
        private void LogHandler(IProducer<Null, string> _, LogMessage message)
        {
            FileLogger.Log($"Producer info [{message.Name}]: " + message.Message);
        }
        private void ErrorHandler(IProducer<Null, string> producer, Error error)
        {
            FileLogger.Log($"Producer error [{producer.Name}]: " + error.Reason);
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