using DaJet.Data.Messaging;
using DaJet.Logging;
using DaJet.Metadata;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.Kafka.Agent
{
    public sealed class MessageProducerService : BackgroundService
    {
        private const string RETRY_MESSAGE_TEMPLATE = "Message producer service will retry in {0} seconds.";
        private AppSettings Settings { get; set; }
        public MessageProducerService(IOptions<AppSettings> options)
        {
            Settings = options.Value;
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            FileLogger.Log("Message producer service is started.");
            // StartAsync calls and awaits ExecuteAsync inside
            return base.StartAsync(cancellationToken);
        }
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            FileLogger.Log("Message producer service is stopping ...");
            // Do shutdown cleanup here (see HostOptions.ShutdownTimeout)
            FileLogger.Log("Message producer service is stopped.");
            return base.StopAsync(cancellationToken);
        }
        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            // Running the service in the background
            _ = Task.Run(async () => { await DoWork(cancellationToken); }, cancellationToken);
            // Return completed task to let other services to run
            return Task.CompletedTask;
        }
        private async Task DoWork(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    TryDoWork(cancellationToken);
                }
                catch (Exception error)
                {
                    FileLogger.LogException(error);
                }
                await Task.Delay(TimeSpan.FromSeconds(Settings.Periodicity), cancellationToken);
            }
        }
        private void TryDoWork(CancellationToken cancellationToken)
        {
            GetMessagingSettingsWithRetry(out MessagingSettings settings, cancellationToken);

            int produced = 0;

            using (IMessageConsumer consumer = GetDatabaseMessageConsumer(in settings))
            {
                produced = new KafkaMessageProducer(
                    settings.MainNode.BrokerServer,
                    settings.MainNode.BrokerIncomingQueue,
                    settings.MainNode.Code)
                    .Produce(in consumer); // TODO: get topic from settings !
            }

            FileLogger.Log($"{produced} messages produced.");
        }
        private IMessageConsumer GetDatabaseMessageConsumer(in MessagingSettings settings)
        {
            if (Settings.DatabaseProvider == DatabaseProvider.SQLServer)
            {
                return new MsMessageConsumer(Settings.ConnectionString, settings.OutgoingQueue, settings.YearOffset);
            }
            else
            {
                return new PgMessageConsumer(Settings.ConnectionString, settings.OutgoingQueue, settings.YearOffset);
            }
        }
        private void GetMessagingSettingsWithRetry(out MessagingSettings settings, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    new PublicationSettings(Settings.DatabaseProvider, Settings.ConnectionString)
                        .SelectMessagingSettings(Settings.ExchangePlanName, out settings);

                    return;
                }
                catch (Exception error)
                {
                    FileLogger.Log("Message producer service: failed to get messaging settings.");
                    FileLogger.LogException(error);
                }

                FileLogger.Log(string.Format(RETRY_MESSAGE_TEMPLATE, Settings.Periodicity));

                Task.Delay(TimeSpan.FromSeconds(Settings.Periodicity), cancellationToken).Wait();
            }
        }

        // FIXME: broker login and password ?
        private string ParseMessageBrokerUri(string uriString)
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
    }
}