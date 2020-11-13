using System;
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
      //QueueUrl = "https://sqs.us-east-2.amazonaws.com/330261344570/TestingTheQueue";

      var createQueueRequest = new CreateQueueRequest();
      createQueueRequest.QueueName = "TestingQueue123";

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

      // sending the message to the message queue  
      var sendMessageRequest = new SendMessageRequest
      {
        QueueUrl = QueueUrl,
        MessageBody = "This is a simple message queue test"
      };
      var sendMessageResponse = SqsClient.SendMessage(sendMessageRequest);

      Assert.IsNotNull(sendMessageResponse);
    }
  }
}

