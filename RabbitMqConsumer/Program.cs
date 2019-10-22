using System;
using System.IO;

namespace RabbitMqConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var rabbit = Serget.Tools.Queueing.QueueBusFactory.Create("200.196.226.148", null, null))
            {
                const string QueueName = "Serget.Indicacao.Exception";
                rabbit.QueueName = QueueName;
                var obj = rabbit.ReceiveMessage<object>();
                int count = 0;

                Console.WriteLine(string.Format("Lendo mensagem das fila {0} ....", QueueName));
                while (obj != null)
                {
                    
                    Console.WriteLine("Carregando mensagem para arquivo de texto");
                    string message = obj.ToString();
                    Directory.CreateDirectory(string.Format(@"c:/jsonfiles/rabbitmq/{0}", QueueName));
                    var filename = string.Format(@"c:/jsonfiles/rabbitmq/{0}/{1}.txt", QueueName, Guid.NewGuid());
                    File.WriteAllText(filename, message);
                    Console.WriteLine(string.Format("Gerado arquivo: {0}", filename));
                    obj = rabbit.ReceiveMessage<object>();
                    count++;
                }

                Console.WriteLine(string.Format("Foram gerados {0} arquivos referente a fila {1}", count, QueueName));
            }
            
            Console.Read();
        }
    }
}
