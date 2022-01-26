using DaJet.Data.Messaging;
using DaJet.Logging;
using DaJet.Metadata;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.Kafka.Agent
{
    public sealed class MessageProducerService : BackgroundService
    {
        private const string DELAY_MESSAGE_TEMPLATE = "Message producer service delay for {0} seconds.";
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
                FileLogger.Log(string.Format(DELAY_MESSAGE_TEMPLATE, Settings.Periodicity));
                await Task.Delay(TimeSpan.FromSeconds(Settings.Periodicity), cancellationToken);
            }
        }
        private void TryDoWork(CancellationToken cancellationToken)
        {
            GetMessagingSettingsWithRetry(out MessagingSettings settings, cancellationToken);

            ConfigureMessageTypeToTopicLookup(in settings, out Dictionary<string, string> lookup);

            int produced = 0;

            using (IMessageConsumer consumer = GetDatabaseMessageConsumer(in settings))
            {
                produced = new KafkaMessageProducer(
                    settings.MainNode.BrokerServer,
                    settings.MainNode.BrokerIncomingQueue,
                    settings.MainNode.Code)
                    .Produce(in consumer, in lookup);
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
        private void ConfigureMessageTypeToTopicLookup(in MessagingSettings settings, out Dictionary<string, string> lookup)
        {
            lookup = new Dictionary<string, string>();

            foreach (NodePublication publication in settings.MainNode.Publications)
            {
                lookup.TryAdd(publication.MessageType, publication.BrokerQueue);
            }
        }
    }
}