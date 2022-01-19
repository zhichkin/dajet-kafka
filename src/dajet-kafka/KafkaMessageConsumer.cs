using Confluent.Kafka;
using DaJet.Data.Messaging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace DaJet.Kafka
{
    public sealed class KafkaMessageConsumer
    {
        private readonly string _topic;
        private readonly string _server;
        private readonly string _client;
        private readonly ConsumerConfig _config;
        private ConsumeResult<Ignore, string> _result;
        private readonly IncomingMessage _message = new IncomingMessage();
        
        private int _batchSize = 1000;

        public KafkaMessageConsumer(in string server, in string topic, in string client)
        {
            _topic = topic;
            _server = server;
            _client = client;

            _config = new ConsumerConfig()
            {
                GroupId = _client,
                ClientId = _client,
                BootstrapServers = _server,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SessionTimeoutMs = 60000,
                HeartbeatIntervalMs = 20000
            };
        }
        public int Consume(in IMessageProducer producer)
        {
            int total = 0;
            
            using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                int consumed = 0;

                consumer.Subscribe(_topic);

                do
                {
                    consumed = ConsumeBatch(in consumer, in producer);

                    total += consumed;
                }
                while (consumed > 0);
            }

            return total;
        }
        private int ConsumeBatch(in IConsumer<Ignore, string> consumer, in IMessageProducer producer)
        {
            int consumed = 0;

            producer.TxBegin();

            do
            {
                _result = consumer.Consume(TimeSpan.FromSeconds(1));

                if (_result != null && _result.Message != null)
                {
                    ProduceDatabaseMessage();

                    producer.Insert(in _message);

                    consumed++;
                }
            }
            while (_result != null && _result.Message != null && consumed <= _batchSize);

            if (consumed > 0)
            {
                producer.TxCommit();
                consumer.Commit();
            }
            else
            {
                consumer.Close();
            }

            return consumed;
        }
        private void ProduceDatabaseMessage()
        {
            _message.Sender = string.Empty;
            _message.Headers = string.Empty;
            _message.MessageType = string.Empty;
            _message.Version = string.Empty;

            SetDatabaseMessageHeaders();

            _message.DateTimeStamp = DateTime.Now;
            _message.MessageBody = _result.Message.Value;
        }
        private void SetDatabaseMessageHeaders()
        {
            if (_result.Message.Headers == null || _result.Message.Headers.Count == 0)
            {
                return;
            }

            Dictionary<string, string> headers = new Dictionary<string, string>();

            for (int i = 0; i < _result.Message.Headers.Count; i++)
            {
                IHeader header = _result.Message.Headers[i];

                if (!TrySetDatabaseMessageProperty(in header))
                {
                    _ = headers.TryAdd(header.Key, Encoding.UTF8.GetString(header.GetValueBytes()));
                }
            }

            if (headers.Count > 0)
            {
                _message.Headers = JsonSerializer.Serialize(headers);
            }
        }
        private bool TrySetDatabaseMessageProperty(in IHeader header)
        {
            if (header.Key == "Sender")
            {
                _message.Sender = Encoding.UTF8.GetString(header.GetValueBytes());
                return true;
            }
            else if (header.Key == "MessageType")
            {
                _message.MessageType = Encoding.UTF8.GetString(header.GetValueBytes());
                return true;
            }
            else if (header.Key == "Version")
            {
                _message.Version = Encoding.UTF8.GetString(header.GetValueBytes());
                return true;
            }
            
            return false;
        }
    }
}