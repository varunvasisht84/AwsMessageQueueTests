using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AwsMessageQueueTests
{
  [TestClass]
  public class AmazonSQSTests
  {
    [TestMethod]
    public void TestCreateMessageQueue()
    {
      AmazonSQSConfig SqsConfig = new AmazonSQSConfig
      {
        ServiceURL = "http://sqs.us-east-2.amazonaws.com"
      };

      AmazonSQSClient SqsClient = new AmazonSQSClient(SqsConfig);

      var createQueueRequest = new CreateQueueRequest();
      createQueueRequest.QueueName = "TestingTheQueue";

      var createQueueResponse = SqsClient.CreateQueue(createQueueRequest);

      Assert.IsNotNull(createQueueResponse);
    }

    [TestMethod]
    public void TestSendTheMessageToQueue()
    {
      AmazonSQSConfig SqsConfig = new AmazonSQSConfig
      {
        ServiceURL = "http://sqs.us-east-2.amazonaws.com"
      };

      AmazonSQSClient SqsClient = new AmazonSQSClient(SqsConfig);
      string QueueUrl = "https://sqs.us-east-2.amazonaws.com/330261344570/TestingTheQueue";

      // Start Fresh
      ClearMessageQueue(SqsClient, QueueUrl);

      string MessageBodyArg = "This is a simple message queue test";

      // sending the message to the message queue  
      var sendMessageRequest = new SendMessageRequest
      {
        QueueUrl = QueueUrl,
        MessageBody = MessageBodyArg
      };
      var sendMessageResponse = SqsClient.SendMessage(sendMessageRequest);

      Assert.IsNotNull(sendMessageResponse, "Send message test.");

      var receiveMessageRequest = new ReceiveMessageRequest();

      receiveMessageRequest.QueueUrl = QueueUrl;
      var receiveMessageResponse = SqsClient.ReceiveMessage(receiveMessageRequest);
      var messages = receiveMessageResponse.Messages;

      // Get the sent message, if not the test fails.
      Assert.IsTrue(messages.Any(), "Recieve message test.");

      if (messages.Any())
      {
        foreach (var message in messages)
        {
          if (message.Body == MessageBodyArg)
          {
            ProcessAndDelete(message, SqsClient, QueueUrl);
          }
        }
      }
    }

    [TestMethod]
    public void TestSendDistinctMessageToQueue()
    {
      AmazonSQSConfig SqsConfig = new AmazonSQSConfig
      {
        ServiceURL = "http://sqs.us-east-2.amazonaws.com"
      };

      AmazonSQSClient SqsClient = new AmazonSQSClient(SqsConfig);
      string QueueUrl = "https://sqs.us-east-2.amazonaws.com/330261344570/TestingTheQueue";

      // Start Fresh
      ClearMessageQueue(SqsClient, QueueUrl);

      // We are using hash set as we will be using it to assert if all the messages are recieved 
      // in any random order.
      HashSet<string> messagesHashSet = new HashSet<string>();
      messagesHashSet.Add("First Message");
      messagesHashSet.Add("Second Message");
      messagesHashSet.Add("Third Message");

      SendMessageResponse sendMessageResponse;
      int index = 0;
      foreach (var message in messagesHashSet)
      {
        // sending the message to the message queue  
        var sendMessageRequest = new SendMessageRequest
        {
          QueueUrl = QueueUrl,
          MessageBody = message
        };
        sendMessageResponse = SqsClient.SendMessage(sendMessageRequest);

        Assert.IsNotNull(sendMessageResponse, string.Format("Send message{0} test.", index++));
      }

      var receiveMessageRequest = new ReceiveMessageRequest();
      index = 0;
      while(true)
      {
        receiveMessageRequest.QueueUrl = QueueUrl;
        var receiveMessageResponse = SqsClient.ReceiveMessage(receiveMessageRequest);
        var messages = receiveMessageResponse.Messages;

        if(!messages.Any())
        {
          break;
        }
        index++;

        foreach (var message in messages)
        {
          messagesHashSet.Add(message.Body);
          ProcessAndDelete(message, SqsClient, QueueUrl);
        }
      }

      // Test the sent messages recieved as distinct.
      Assert.IsTrue(index == 3, "Same number of Messages Recieved as Sent");

      // If the hashset contains all the sent messages after adding the recieved message,
      // we can be assured that no extra message has been recieved
      Assert.IsTrue(messagesHashSet.Count == 3, "Exact same messages test recieved as sent");
    }

    private static void ProcessAndDelete(Message message, AmazonSQSClient SqsClient, string QueueUrl)
    {
      // Process message
      var receiptHandle = message.ReceiptHandle;

      // Delete message
      var deleteMessageRequest = new DeleteMessageRequest();

      deleteMessageRequest.QueueUrl = QueueUrl;
      deleteMessageRequest.ReceiptHandle = receiptHandle;

      var response = SqsClient.DeleteMessage(deleteMessageRequest);
    }

    private static void ClearMessageQueue(AmazonSQSClient SqsClient, string QueueUrl)
    {
      var receiveMessageRequest = new ReceiveMessageRequest();
      while (true)
      {
        receiveMessageRequest.QueueUrl = QueueUrl;
        var receiveMessageResponse = SqsClient.ReceiveMessage(receiveMessageRequest);
        var messages = receiveMessageResponse.Messages;

        if (!messages.Any())
        {
          break;
        }

        foreach (var message in messages)
        {
          ProcessAndDelete(message, SqsClient, QueueUrl);
        }
      }
    }

    [TestMethod]
    public void TestSendBatchMessageToQueue()
    {
      AmazonSQSConfig SqsConfig = new AmazonSQSConfig
      {
        ServiceURL = "http://sqs.us-east-2.amazonaws.com"
      };

      AmazonSQSClient SqsClient = new AmazonSQSClient(SqsConfig);
      string QueueUrl = "https://sqs.us-east-2.amazonaws.com/330261344570/TestingTheQueue";

      ClearMessageQueue(SqsClient, QueueUrl);

      // We are using hash set as we will be using it to assert if all the messages are recieved 
      // in any random order.
      HashSet<string> messagesHashSet = new HashSet<string>();
      messagesHashSet.Add("First Message");
      messagesHashSet.Add("Second Message");
      messagesHashSet.Add("Third Message");

      // Since indexing cant be applied on HashSet, this is a temporary variable taken
      var messagesArray = messagesHashSet.ToArray();

      var sendMessageBatchRequest = new SendMessageBatchRequest
      {
        Entries = new List<SendMessageBatchRequestEntry>
        {
            new SendMessageBatchRequestEntry("message1", messagesArray[0]),
            new SendMessageBatchRequestEntry("message2", messagesArray[1]),
            new SendMessageBatchRequestEntry("message3", messagesArray[2])
        },
        QueueUrl = QueueUrl,
      };

      var sendMessageResponse = SqsClient.SendMessageBatch(sendMessageBatchRequest);

      Assert.IsNotNull(sendMessageResponse, "Send message test.");

      var receiveMessageRequest = new ReceiveMessageRequest();
      int index = 0;
      while (true)
      {
        receiveMessageRequest.QueueUrl = QueueUrl;
        var receiveMessageResponse = SqsClient.ReceiveMessage(receiveMessageRequest);
        var messages = receiveMessageResponse.Messages;

        if (!messages.Any())
        {
          break;
        }
        index++;

        foreach (var message in messages)
        {
          messagesHashSet.Add(message.Body);
          ProcessAndDelete(message, SqsClient, QueueUrl);
        }
      }

      // Test the sent messages recieved as distinct.
      Assert.IsTrue(index == 3, "Same number of Messages Recieved as Sent");

      // If the hashset contains all the sent messages after adding the recieved message,
      // we can be assured that no extra message has been recieved
      Assert.IsTrue(messagesHashSet.Count == 3, "Exact same messages test recieved as sent");
    }
  }
}

