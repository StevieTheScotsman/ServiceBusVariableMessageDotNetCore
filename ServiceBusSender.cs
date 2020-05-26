using Microsoft.Azure.ServiceBus;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;

/* https://www.serverless360.com/blog/deal-with-large-service-bus-messages-using-claim-check-pattern */

/* TODO implement base constructor as time allows */

namespace MessageQueueClient.Messaging
{

    public  class ServiceBusSender
    {
        
        #region variables

        private readonly string _serviceBusConnString;
        private readonly string _storageConnString;
        private readonly string _serviceBusQueueName;
        private readonly string _storageContainerName;
       

        static IQueueClient _queueClient;
        
        #endregion

        #region constructor
        public ServiceBusSender(string serviceBusConnString,string storageConnString,string serviceBusQueueName,string storageContainerName)
        {
            _serviceBusConnString = serviceBusConnString;
            _storageConnString = storageConnString;
            _serviceBusQueueName = serviceBusQueueName;
            _storageContainerName = storageContainerName;

        }

        #endregion

        /* I added this on the offchance that we may want to use this on the calling side */
        #region public methods
        public bool CanSendOnServiceBusQueue(object userMsg)
        {
            string serialBody = JsonConvert.SerializeObject(userMsg);
            byte[] body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(serialBody));

            Message message = new Message { Body = body };

            return CanSendOnServiceBusQueue(message);

        }

        public async Task SendAsync(Message message)
        {
            _queueClient = new QueueClient(_serviceBusConnString, _serviceBusQueueName);

            try
            {

                if(CanSendOnServiceBusQueue(message))
                {
                    message.UserProperties["MessageType"] = Constants.ServiceBusMessageType;

                }

                else
                {
                    message.UserProperties["MessageType"] = Constants.StorageBlobMessageType;

                    // get reference to a storage account use blob name for message id
                    CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(_storageConnString);

                    // create cloud queue client
                    CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

                    // put message in blob storage..blob reference will be same as message id.
                    CloudBlobContainer container = cloudBlobClient.GetContainerReference(_storageContainerName);
                    string blobName = message.MessageId;

                    await container.CreateIfNotExistsAsync();
                    CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(blobName);

                    await cloudBlockBlob.UploadFromByteArrayAsync(message.Body, 0, message.Body.Length);

                    //we will make the message body empty

                    message.Body = new byte[0];

                    await _queueClient.SendAsync(message);


                }

            }

            catch(Exception e)
            {
                //log to microservice

            }

            finally
            {
                //possible cleanup

            }

            
        }

        #endregion

        #region private methods
        private bool CanSendOnServiceBusQueue(Message message)
        {
            return message.Size < Constants.MaxMessageSizeInBytes;
        }
        #endregion

    }



}