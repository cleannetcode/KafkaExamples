using Confluent.Kafka;

var tokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
	tokenSource.Cancel();
};

var producerConfig = new ProducerConfig
{
	BootstrapServers = "localhost:9092",
};

var producer = new ProducerBuilder<Null, string>(producerConfig)
	.Build();

const string topic = "test-topic";


while (!tokenSource.IsCancellationRequested)
{
	var message = new Message<Null, string>
	{
		Value = Guid.NewGuid().ToString()
	};

	producer.Produce(topic, message, deliveryResult =>
	{
		Console.WriteLine(deliveryResult.Message.Value);
	});


	await Task.Delay(1000, tokenSource.Token);
}

