using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.IO;
using System.Text;

namespace RabbitMqConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            const string rabbitConn = "localhost";

            var factory = new ConnectionFactory
            {
                HostName = rabbitConn
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                const string QueueName = "Teste";
                channel.QueueDeclare(
                    queue: QueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                    );

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine("Carregando mensagem para arquivo de texto");
                    Directory.CreateDirectory(string.Format(@"c:/jsonfiles/rabbitmq/{0}", QueueName));
                    var filename = string.Format(@"c:/jsonfiles/rabbitmq/{0}/{1}.txt", QueueName, Guid.NewGuid());
                    File.WriteAllText(filename, message);
                    Console.WriteLine(string.Format("Gerado arquivo: {0}", filename));
                };
                channel.BasicConsume(queue: QueueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
