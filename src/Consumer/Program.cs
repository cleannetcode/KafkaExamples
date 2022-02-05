using Confluent.Kafka;

var tokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
	tokenSource.Cancel();
};

var consumerConfig = new ConsumerConfig
{
	BootstrapServers = "localhost:9092",
	GroupId = args[0] ?? "test-consumer-group",
	AutoOffsetReset = AutoOffsetReset.Earliest,
	EnableAutoOffsetStore = false
};

var consumer = new ConsumerBuilder<Null, string>(consumerConfig)
	.Build();

const string topic = "test-topic";
consumer.Subscribe(topic);

var random = new Random();

while (!tokenSource.IsCancellationRequested)
{
	var message = consumer.Consume(new TimeSpan(0, 0, 1));

	if (message != null)
	{
		try
		{
			if (random.Next(0, 2) == 0)
			{
				Console.WriteLine(message.TopicPartitionOffset + " " + message.Value);
			}
			else
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine(message.TopicPartitionOffset + " " + message.Value);
				Console.ResetColor();
				throw new Exception("Error");
			}

			consumer.StoreOffset(message);
		}
		catch (Exception)
		{
			consumer.Seek(message.TopicPartitionOffset);
		}
	}
}

consumer.Close();
