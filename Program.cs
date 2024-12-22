using Confluent.Kafka;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

namespace RedisKafkaQualityManager
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables();
            var configuration = configBuilder.Build();

            var kafkaSection = configuration.GetSection("Kafka");
            string bootstrapServers = kafkaSection.GetValue<string>("BootstrapServers");
            string groupId = kafkaSection.GetValue<string>("GroupId");
            string metricTopic = kafkaSection.GetValue<string>("MetricTopic");
            string qualityManagerTopic = kafkaSection.GetValue<string>("QualityManagerTopic");

            var redisSection = configuration.GetSection("Redis");
            string redisHost = redisSection.GetValue<string>("Host");
            int redisPort = redisSection.GetValue<int>("Port");
            int redisDb = redisSection.GetValue<int>("Db");

            var processingSection = configuration.GetSection("Processing");
            int videoProcessingServicesCount = processingSection.GetValue<int>("VideoProcessingServicesCount", 10);
            int highLoadThresholdPercent = processingSection.GetValue<int>("HighLoadThresholdPercent", 90);

            var redis = ConnectionMultiplexer.Connect($"{redisHost}:{redisPort}");
            IDatabase db = redis.GetDatabase(redisDb);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            Console.WriteLine("Service is running. Waiting for messages from Kafka...");

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            consumer.Subscribe(metricTopic);

            using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (cr == null) 
                            continue;

                        if (cr.Message != null)
                        {
                            var messageValue = cr.Message.Value;
                            Console.WriteLine($"Received message from Kafka: {messageValue}");

                            try
                            {
                                var data = JsonSerializer.Deserialize<MetricData>(messageValue);
                                if (data == null)
                                    continue;

                                ProcessAndStoreInRedis(db, data);

                                var loadInfo = CalculateLoad(data, videoProcessingServicesCount);

                                string recommendedQuality = DetermineQuality(loadInfo);

                                var messageToQualityMgr = new QualityMessage
                                {
                                    UserId = data.UserId,
                                    Quality = recommendedQuality
                                };

                                string serializedQualityMsg = JsonSerializer.Serialize(messageToQualityMgr);
                                
                                await producer.ProduceAsync(
                                    qualityManagerTopic,
                                    new Message<Null, string> { Value = serializedQualityMsg }
                                );
                                Console.WriteLine($"Sent message to {qualityManagerTopic}: {serializedQualityMsg}");

                                if (loadInfo.LoadPercent > highLoadThresholdPercent)
                                {
                                    var criticalMsg = new QualityMessage
                                    {
                                        UserId = data.UserId,
                                        Quality = "critical"
                                    };

                                    await producer.ProduceAsync(
                                        qualityManagerTopic,
                                        new Message<Null, string> { Value = JsonSerializer.Serialize(criticalMsg) }
                                    );
                                    Console.WriteLine($"Sent CRITICAL message to {qualityManagerTopic}");
                                }
                            }
                            catch (JsonException ex)
                            {
                                Console.WriteLine($"JSON parse error: {ex.Message}");
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Kafka ConsumeException: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Stopping Consumer...");
            }
            finally
            {
                consumer.Close();
            }
        }

        static void ProcessAndStoreInRedis(IDatabase db, MetricData data)
        {
            string key = $"vod_data:{data.UserId}";

            if (DateTime.TryParse(data.Timestamp, out DateTime parsedTime))
            {
                double timestampScore = parsedTime.ToUniversalTime().Subtract(
                    new DateTime(1970, 1, 1)
                ).TotalSeconds;

                string jsonData = JsonSerializer.Serialize(data);

                db.SortedSetAdd(key, jsonData, timestampScore);

                Console.WriteLine($"Record {key} with timestamp {data.Timestamp} saved to Redis.");
            }
            else
            {
                Console.WriteLine($"Failed to parse date: {data.Timestamp}");
            }
        }

        static LoadInfo CalculateLoad(MetricData data, int processingServicesCount)
        {
            double loadPercent = (data.VideosUploaded / (processingServicesCount * 10.0)) * 100.0;

            return new LoadInfo
            {
                LoadPercent = loadPercent
            };
        }

        static string DetermineQuality(LoadInfo loadInfo)
        {
            if (loadInfo.LoadPercent < 30)
            {
                return "1080p";
            }
            else if (loadInfo.LoadPercent < 70)
            {
                return "720p";
            }
            else
            {
                return "480p";
            }
        }
    }

    public class MetricData
    {
        public string UserId { get; set; } = string.Empty;
        public string Timestamp { get; set; } = string.Empty;
        public int VideosUploaded { get; set; }
    }

    public class LoadInfo
    {
        public double LoadPercent { get; set; }
    }

    public class QualityMessage
    {
        public string UserId { get; set; } = string.Empty;
        public string Quality { get; set; } = string.Empty;
    }
}
