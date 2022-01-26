using DaJet.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DaJet.Kafka.Agent
{
    public static class Program
    {
        public static void Main()
        {
            FileLogger.UseFileName("dajet-kafka-agent");

            FileLogger.Log("Hosting service is started.");
            CreateHostBuilder().Build().Run();
            FileLogger.Log("Hosting service is stopped.");
        }
        private static IHostBuilder CreateHostBuilder()
        {
            return Host.CreateDefaultBuilder()
                .UseSystemd()
                .UseWindowsService()
                .ConfigureAppConfiguration(config =>
                {
                    config.Sources.Clear();
                    config.AddJsonFile("appsettings.json", optional: false);
                })
                .ConfigureServices(ConfigureServices);
        }
        private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            services
                .AddOptions()
                .Configure<AppSettings>(context.Configuration)
                .Configure<HostOptions>(context.Configuration.GetSection(nameof(HostOptions)))
                .AddHostedService<MessageProducerService>()
                .AddHostedService<MessageConsumerService>();
        }
    }
}