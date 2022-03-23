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
    public sealed class MessageConsumerService : BackgroundService
    {
        private const string DELAY_MESSAGE_TEMPLATE = "Message consumer service delay for {0} seconds.";
        private const string RETRY_MESSAGE_TEMPLATE = "Message consumer service will retry in {0} seconds.";
        private AppSettings Settings { get; set; }
        public MessageConsumerService(IOptions<AppSettings> options)
        {
            Settings = options.Value;
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            FileLogger.Log("Message consumer service is started.");
            // StartAsync calls and awaits ExecuteAsync inside
            return base.StartAsync(cancellationToken);
        }
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            FileLogger.Log("Message consumer service is stopping ...");
            // Do shutdown cleanup here (see HostOptions.ShutdownTimeout)
            FileLogger.Log("Message consumer service is stopped.");
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

            int consumed = 0;

            using (IMessageProducer producer = GetDatabaseMessageProducer(in settings))
            {
                // TODO: create consumer for each topic (subscription setting) !?
                consumed = new KafkaMessageConsumer(
                    settings.MainNode.BrokerServer,
                    settings.MainNode.BrokerOutgoingQueue,
                    settings.MainNode.Code)
                    .Consume(in producer);
            }

            FileLogger.Log($"{consumed} messages consumed.");
        }
        private IMessageProducer GetDatabaseMessageProducer(in MessagingSettings settings)
        {
            if (Settings.DatabaseProvider == DatabaseProvider.SQLServer)
            {
                return new MsMessageProducer(Settings.ConnectionString, settings.IncomingQueue);
            }
            else
            {
                return new PgMessageProducer(Settings.ConnectionString, settings.IncomingQueue);
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
                    FileLogger.Log("Message consumer service: failed to get messaging settings.");
                    FileLogger.LogException(error);
                }

                FileLogger.Log(string.Format(RETRY_MESSAGE_TEMPLATE, Settings.Periodicity));

                Task.Delay(TimeSpan.FromSeconds(Settings.Periodicity), cancellationToken).Wait();
            }
        }
    }
}