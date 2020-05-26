using Microsoft.Azure.ServiceBus;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;


/* https://www.serverless360.com/blog/deal-with-large-service-bus-messages-using-claim-check-pattern 
 https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dotnet-get-started-with-queues 
 https://medium.com/bynder-tech/c-why-you-should-use-configureawait-false-in-your-library-code-d7837dce3d7f*/


namespace MessageQueueClient.Messaging
{
    public class ServiceBusReceiver
    {
        #region variables

        private readonly string _serviceBusConnString;
        private readonly string _storageConnString;
        private readonly string _serviceBusQueueName;
        private readonly string _storageContainerName;
        
        private readonly int _maxConcurrentCalls;

        static IQueueClient _queueClient;


        #endregion

        #region constructor
        public ServiceBusReceiver(string serviceBusConnString, string storageConnString, string serviceBusQueueName, string storageContainerName, int maxConcurrentCalls)
        {
            _serviceBusConnString = serviceBusConnString;
            _storageConnString = storageConnString;
            _serviceBusQueueName = serviceBusQueueName;
            _storageContainerName = storageContainerName;
            _maxConcurrentCalls = maxConcurrentCalls;

        }

        #endregion

        #region private methods
        private bool IsOversizedMessage(Message message)
        {
            return message.UserProperties["MessageType"].ToString() == Constants.ServiceBusMessageType;
        }

        async Task ReceiveAsync()
        {
            _queueClient = new QueueClient(_serviceBusConnString, _serviceBusQueueName, ReceiveMode.PeekLock, RetryPolicy.Default);

            // Register QueueClient's MessageHandler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            await _queueClient.CloseAsync();
        }

        
        private async Task<Message> ProcessMessagesAsync(Message message, CancellationToken token)
        {
            
            // Process the message.
            if(IsOversizedMessage(message))
            {
                // get storage account
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(_storageConnString);

                // get cloudblobclient
                CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

                // get message from blob storage
                CloudBlobContainer container = cloudBlobClient.GetContainerReference(_storageContainerName);
                CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(message.MessageId);

                long fileByteLength = cloudBlockBlob.Properties.Length;
                byte[] bytes = new byte[fileByteLength];
                await cloudBlockBlob.DownloadToByteArrayAsync(bytes, 0).ConfigureAwait(false);

                message.Body = bytes;
                
            }

            // Complete the message so that it is not received again.
            // This can be done only if the queue Client is created in ReceiveMode.PeekLock mode (which is the default).
            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
            // to avoid unnecessary exceptions.

            if(token.CanBeCanceled)
            {
                await _queueClient.CompleteAsync(message.SystemProperties.LockToken).ConfigureAwait(false);
            }

            return message;

        }


        //todo log exception
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            //Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            ExceptionReceivedContext context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            //Console.WriteLine("Exception context for troubleshooting:");
            //Console.WriteLine($"- Endpoint: {context.Endpoint}");
            //Console.WriteLine($"- Entity Path: {context.EntityPath}");
            //Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
        public void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            MessageHandlerOptions messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = _maxConcurrentCalls,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };

            // Register the function that processes messages.
            _queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }



        #endregion

    }

}