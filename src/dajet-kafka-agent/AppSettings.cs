using DaJet.Metadata;

namespace DaJet.Kafka.Agent
{
    public sealed class AppSettings
    {
        ///<summary>Ограничение размера лога службы в килобайтах</summary>
        public int LogSize { get; set; } = 1024 * 1024; // 1 Mb
        ///<summary>Периодичность обработки входящих и исходящих очередей СУБД в секундах</summary>
        public int Periodicity { get; set; } = 60;
        ///<summary>Тип СУБД</summary>
        public DatabaseProvider DatabaseProvider { get; set; } = DatabaseProvider.SQLServer;
        ///<summary>Строка подключения СУБД</summary>
        public string ConnectionString { get; set; } = string.Empty;
        ///<summary>
        ///<br>Имя плана обмена 1С:Предприятие 8 как оно задано в конфигураторе.</br>
        ///<br>Используется для чтения настроек подключения к брокеру сообщений и т.п.</br>
        ///</summary>
        public string ExchangePlanName { get; set; } = string.Empty;
    }
}