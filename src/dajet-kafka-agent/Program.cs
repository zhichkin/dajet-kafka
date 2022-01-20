using DaJet.Data.Messaging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace DaJet.Kafka.Agent
{
    public static class Program
    {
        private static string KAFKA_TOPIC;
        private static string KAFKA_SERVER;
        private static string KAFKA_CLIENT;

        private const string PUBLICATION_NAME = "ПланОбмена.DaJetMessaging";
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";
        
        private static readonly DbInterfaceValidator _validator = new DbInterfaceValidator();
        private static readonly MsQueueConfigurator _ms_configurator = new MsQueueConfigurator(MS_CONNECTION_STRING);
        private static readonly PgQueueConfigurator _pg_configurator = new PgQueueConfigurator(PG_CONNECTION_STRING);

        private static int _YearOffset = 0;
        private static ApplicationObject _incomingQueue;
        private static ApplicationObject _outgoingQueue;

        public static void Main()
        {
            string connectionString = MS_CONNECTION_STRING;
            DatabaseProvider provider = DatabaseProvider.SQLServer;

            //string connectionString = PG_CONNECTION_STRING;
            //DatabaseProvider provider = DatabaseProvider.PostgreSQL;

            Console.WriteLine($"OS version: {Environment.OSVersion}");

            if (!new MetadataService()
                .UseDatabaseProvider(provider)
                .UseConnectionString(connectionString)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                Console.WriteLine(message);
                return;
            }

            _YearOffset = infoBase.YearOffset;

            PublicationNode settings = GetPublicationSettings(provider, in connectionString);

            KAFKA_CLIENT = settings.Code;
            KAFKA_TOPIC = settings.BrokerIncomingQueue;
            KAFKA_SERVER = ParseBrokerServerUri(settings.BrokerServer);

            _outgoingQueue = GetOutgoingQueueMetadata(infoBase, settings);
            ValidateOutgoingInterface(in _outgoingQueue);
            if (provider == DatabaseProvider.SQLServer)
            {
                ConfigureOutgoingQueue(_ms_configurator, in _outgoingQueue);
            }
            else
            {
                ConfigureOutgoingQueue(_pg_configurator, in _outgoingQueue);
            }

            _incomingQueue = GetIncomingQueueMetadata(infoBase, settings);
            ValidateIncomingInterface(in _incomingQueue);
            if (provider == DatabaseProvider.SQLServer)
            {
                ConfigureIncomingQueue(_ms_configurator, in _incomingQueue);
            }
            else
            {
                ConfigureIncomingQueue(_pg_configurator, in _incomingQueue);
            }

            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();
                
                Produce(provider, in connectionString);
                //Consume(provider, in connectionString);

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

        private static string ParseBrokerServerUri(string uriString)
        {
            // amqp://guest:guest@localhost:5672/%2F/РИБ.ERP

            Uri uri = new Uri(uriString);

            if (uri.Scheme != "kafka")
            {
                throw new FormatException(nameof(uriString));
            }

            return $"{uri.Host}:{uri.Port}";

            //HostName = uri.Host;
            //HostPort = uri.Port;

            //string[] userpass = uri.UserInfo.Split(':');
            //if (userpass != null && userpass.Length == 2)
            //{
            //    UserName = HttpUtility.UrlDecode(userpass[0], Encoding.UTF8);
            //    Password = HttpUtility.UrlDecode(userpass[1], Encoding.UTF8);
            //}

            //if (uri.Segments != null && uri.Segments.Length == 3)
            //{
            //    if (uri.Segments.Length > 1)
            //    {
            //        VirtualHost = HttpUtility.UrlDecode(uri.Segments[1].TrimEnd('/'), Encoding.UTF8);
            //    }

            //    if (uri.Segments.Length == 3)
            //    {
            //        ExchangeName = HttpUtility.UrlDecode(uri.Segments[2].TrimEnd('/'), Encoding.UTF8);
            //    }
            //}
        }

        private static PublicationNode GetPublicationSettings(DatabaseProvider provider, in string connectionString)
        {
            if (!new MetadataService()
                .UseDatabaseProvider(provider)
                .UseConnectionString(connectionString)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                throw new Exception(message);
            }

            _YearOffset = infoBase.YearOffset;

            ApplicationObject metadata = infoBase.GetApplicationObjectByName(PUBLICATION_NAME);

            Publication publication = metadata as Publication;
            if (publication == null)
            {
                throw new Exception($"Publication \"{PUBLICATION_NAME}\" is not found.");
            }

            TablePart publications = publication.TableParts.Where(t => t.Name == "ИсходящиеСообщения").FirstOrDefault();
            TablePart subscriptions = publication.TableParts.Where(t => t.Name == "ВходящиеСообщения").FirstOrDefault();

            PublicationSettings settings = new PublicationSettings(provider, connectionString);
            settings.Select(in publication);

            PublicationNode node = settings.Select(in publication, publication.Publisher.Uuid);
            if (publications != null)
            {
                node.Publications = settings.SelectNodePublications(publications, node.Uuid);
            }
            if (subscriptions != null)
            {
                node.Subscriptions = settings.SelectNodeSubscriptions(subscriptions, node.Uuid);
            }

            return node;
        }

        private static ApplicationObject GetOutgoingQueueMetadata(InfoBase infoBase, PublicationNode settings)
        {
            string OUTGOING_QUEUE_NAME = $"РегистрСведений.{settings.NodeOutgoingQueue}";

            ApplicationObject queue = infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);

            if (queue == null)
            {
                throw new Exception($"Outgoing queue \"{OUTGOING_QUEUE_NAME}\" is not found.");
            }

            return queue;
        }
        private static void ValidateOutgoingInterface(in ApplicationObject queue)
        {
            int version = _validator.GetOutgoingInterfaceVersion(in queue);

            if (version < 1)
            {
                throw new Exception($"Outgoing queue interface is invalid.");
            }
        }
        private static void ConfigureOutgoingQueue(in IQueueConfigurator configurator, in ApplicationObject queue)
        {
            configurator.ConfigureOutgoingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                string message = string.Empty;

                foreach (string error in errors)
                {
                    message += error + Environment.NewLine;
                }

                throw new Exception(message);
            }
        }

        private static ApplicationObject GetIncomingQueueMetadata(InfoBase infoBase, PublicationNode settings)
        {
            string INCOMING_QUEUE_NAME = $"РегистрСведений.{settings.NodeIncomingQueue}";

            ApplicationObject queue = infoBase.GetApplicationObjectByName(INCOMING_QUEUE_NAME);

            if (queue == null)
            {
                throw new Exception($"Incoming queue \"{INCOMING_QUEUE_NAME}\" is not found.");
            }

            return queue;
        }
        private static void ValidateIncomingInterface(in ApplicationObject queue)
        {
            int version = _validator.GetIncomingInterfaceVersion(in queue);

            if (version < 1)
            {
                throw new Exception($"Incoming queue interface is invalid.");
            }
        }
        private static void ConfigureIncomingQueue(in IQueueConfigurator configurator, in ApplicationObject queue)
        {
            configurator.ConfigureIncomingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                string message = string.Empty;

                foreach (string error in errors)
                {
                    message += error + Environment.NewLine;
                }

                throw new Exception(message);
            }
        }

        private static void Produce(DatabaseProvider provider, in string connectionString)
        {
            int total = 0;

            if (provider == DatabaseProvider.SQLServer)
            {
                using (IMessageConsumer consumer = new MsMessageConsumer(connectionString, in _outgoingQueue, _YearOffset))
                {
                    total = new KafkaMessageProducer(KAFKA_SERVER, KAFKA_TOPIC, KAFKA_CLIENT).Produce(in consumer);
                }
            }
            else
            {
                using (IMessageConsumer consumer = new PgMessageConsumer(connectionString, in _outgoingQueue, _YearOffset))
                {
                    total = new KafkaMessageProducer(KAFKA_SERVER, KAFKA_TOPIC, KAFKA_CLIENT).Produce(in consumer);
                }
            }

            Console.WriteLine($"Total = {total}");
        }
        private static void Consume(DatabaseProvider provider, in string connectionString)
        {
            int total = 0;

            if (provider == DatabaseProvider.SQLServer)
            {
                using (IMessageProducer producer = new MsMessageProducer(connectionString, in _incomingQueue, _YearOffset))
                {
                    total = new KafkaMessageConsumer(KAFKA_SERVER, KAFKA_TOPIC, KAFKA_CLIENT).Consume(in producer);
                }
            }
            else
            {
                using (IMessageProducer producer = new PgMessageProducer(connectionString, in _incomingQueue, _YearOffset))
                {
                    total = new KafkaMessageConsumer(KAFKA_SERVER, KAFKA_TOPIC, KAFKA_CLIENT).Consume(in producer);
                }
            }

            Console.WriteLine($"Total = {total}");
        }
    }
}