using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Timers;
using System.Configuration;

namespace QueueGetter
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("*************");
            Console.WriteLine("Welcome to the QueueGetter app!");
            Console.WriteLine("This little app will read messages from an Azure Service Bus queue!");
            Console.WriteLine("*************");
            Console.WriteLine();
            
            Console.WriteLine("Enter 1 for loop via MessageReceiver");
            Console.WriteLine("Enter 2 for message pump via QueueClient");
            string selection = Console.ReadLine();

            if (selection == "1")
            {
                CheckQueueMessageReciever(); //manual loop
            }
            else if (selection == "2")
            {
                CheckQueueQueueClient(); //message pump
            }
            else
            {
                Console.WriteLine("Invalid selection, please try again!");
            }

            Console.WriteLine("Processing stopped");
            Console.ReadLine();
        }

        public static void CheckQueueMessageReciever()
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"]);
            MessageReceiver receiver = factory.CreateMessageReceiver(ConfigurationManager.AppSettings["ServiceBusQueueName"]);

            while (true)
            {
                try
                {
                    BrokeredMessage receivedMessage = null;
                    receivedMessage = receiver.Receive();
                    if (receivedMessage != null)
                    {
                        Console.WriteLine("Processing message: " + receivedMessage.MessageId);
                        Console.WriteLine("Message body: " + receivedMessage.GetBody<string>());
                        receivedMessage.Complete();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Had error: {0}", e);
                    //receivedMessage.Abandon();
                }
            }            
        }

        public static void CheckQueueQueueClient()
        {
            //MessagingFactory factory = MessagingFactory.CreateFromConnectionString(ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"]);
            //QueueClient queueClient = factory.CreateQueueClient(ConfigurationManager.AppSettings["ServiceBusQueueName"]);
            var queueClient = QueueClient.Create(ConfigurationManager.AppSettings["ServiceBusQueueName"]); // note that this pulls conn string from App.config by default

            OnMessageOptions options = new OnMessageOptions();
            options.AutoComplete = true;
            options.MaxConcurrentCalls = 1;
            options.ExceptionReceived += LogErrors;
            queueClient.OnMessage((receivedMessage) =>
            {
                try
                {
                    Console.WriteLine("Proccessing Message: " + receivedMessage.SequenceNumber.ToString());
                    Console.WriteLine(receivedMessage.GetBody<string>());
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e.Message);
                }
            }, options);
        }

        public static void LogErrors(object sender, ExceptionReceivedEventArgs e)
        {
            if (e != null && e.Exception != null)
            {
                Console.WriteLine("Error: " + e.Exception.Message);
            }
        }
    }
}
