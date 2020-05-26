/* They are not currently enums since I wanted to keep the message readable for debugging purposes */

namespace MessageQueueClient.Messaging
{
     static class Constants
    {
        public static readonly int MaxMessageSizeInBytes = 200 * 1024;
        public static  readonly  string ServiceBusMessageType = "SERVICE";
        public static readonly string StorageBlobMessageType = "STORAGE";
    }

}

