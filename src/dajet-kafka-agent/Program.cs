using DaJet.Data.Messaging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace DaJet.Kafka.Agent
{
    public static class Program
    {
        private const string KAFKA_CLIENT = "test";
        private const string KAFKA_TOPIC = "test-topic";
        private const string KAFKA_GROUP = "dajet";
        private const string KAFKA_SERVER = "172.31.163.221:9092"; //"Zhichkin.localdomain:9092"; //"localhost:9092";

        private static readonly DbInterfaceValidator _validator = new DbInterfaceValidator();
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";
        private static readonly MsQueueConfigurator _ms_configurator = new MsQueueConfigurator(MS_CONNECTION_STRING);
        private static readonly PgQueueConfigurator _pg_configurator = new PgQueueConfigurator(PG_CONNECTION_STRING);

        private static int _YearOffset = 0;
        private static ApplicationObject _incomingQueue;
        private static ApplicationObject _outgoingQueue;

        public static void Main()
        {
            Console.WriteLine($"OS version: {Environment.OSVersion}");

            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                Console.WriteLine(message);
                return;
            }

            _YearOffset = infoBase.YearOffset;
            _incomingQueue = infoBase.GetApplicationObjectByName("РегистрСведений.ВходящаяОчередь");
            _outgoingQueue = infoBase.GetApplicationObjectByName("РегистрСведений.ИсходящаяОчередь");

            try
            {
                ValidateDatabaseInterface(in _incomingQueue, in _outgoingQueue);
                ConfigureIncomingQueue(in _incomingQueue);
                ConfigureOutgoingQueue(in _outgoingQueue);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return;
            }
            
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();
                
                Produce();
                //Consume();

                watch.Stop();
                Console.WriteLine($"Elapsed = {watch.ElapsedMilliseconds} ms");
            }
            catch (Exception error)
            {
                Console.WriteLine(error.Message);
            }

            Console.WriteLine();
            Console.WriteLine("Press any key to exit ...");
            Console.ReadKey(false);
        }

        private static void ValidateDatabaseInterface(in ApplicationObject incomingQueue, in ApplicationObject outgoingQueue)
        {
            int version = -1;

            version = _validator.GetIncomingInterfaceVersion(in incomingQueue);
            if (version < 1)
            {
                throw new Exception($"Incoming queue interface is invalid.");
            }

            version = _validator.GetOutgoingInterfaceVersion(in outgoingQueue);
            if (version < 1)
            {
                throw new Exception($"Outgoing queue interface is invalid.");
            }
        }
        private static void ConfigureIncomingQueue(in ApplicationObject incomingQueue)
        {
            _ms_configurator.ConfigureIncomingMessageQueue(in incomingQueue, out List<string> errors);

            if (errors.Count > 0)
            {
                string message = string.Empty;

                foreach (string error in errors)
                {
                    message += error + Environment.NewLine;
                }

                throw new Exception(message);
            }
            else
            {
                Console.WriteLine("Incoming queue configured successfully.");
            }
        }
        private static void ConfigureOutgoingQueue(in ApplicationObject outgoingQueue)
        {
            _ms_configurator.ConfigureOutgoingMessageQueue(in outgoingQueue, out List<string> errors);

            if (errors.Count > 0)
            {
                string message = string.Empty;

                foreach (string error in errors)
                {
                    message += error + Environment.NewLine;
                }

                throw new Exception(message);
            }
            else
            {
                Console.WriteLine("Outgoing queue configured successfully.");
            }
        }

        private static void Produce()
        {
            int total = 0;

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in _outgoingQueue, _YearOffset))
            {
                total = new KafkaMessageProducer(KAFKA_SERVER, KAFKA_TOPIC, KAFKA_CLIENT).Produce(in consumer);
            }

            Console.WriteLine($"Total = {total}");
        }
        private static void Consume()
        {
            int total = 0;

            using (IMessageProducer producer = new MsMessageProducer(MS_CONNECTION_STRING, in _incomingQueue, _YearOffset))
            {
                total = new KafkaMessageConsumer(KAFKA_SERVER, KAFKA_CLIENT, KAFKA_TOPIC, KAFKA_GROUP).Consume(in producer);
            }

            Console.WriteLine($"Total = {total}");
        }
    }
}