namespace AWSSQSService
{
    public class SqsService : ISqsService
    {
        private AmazonSQSClient _sqsClient;
        private string SqsUrl { get; set; }
        private string ReceiptHandle { get; set; }
        public List<string> SqsAttributesToRetrieve { get; set; }

        public SqsService(string? proxyURL)
        {
            SetupSqsClient(proxyURL);

            SqsAttributesToRetrieve = new List<string>()
            {
                Constants.VISIBILITY_TIMEOUT
            };
        }

        /// <summary>
        /// This method sets client properties: SqsUrl, ReceipHandler
        /// </summary>
        /// <param name="message" type="SQSEvent.SQSMessage"></param>
        public void SetupClientProperties(SQSEvent.SQSMessage message)
        {
            SetupSqsUrl(message);
            SetupReceipHandler(message);
        }

        private void SetupSqsClient(string? proxyURL)
        {
            var sQsConfig = new AmazonSQSConfig();

            if (!string.IsNullOrWhiteSpace(proxyURL))
            {
                sQsConfig.SetWebProxy(new WebProxy(proxyURL, true));
            }
            _sqsClient = new AmazonSQSClient(sQsConfig);
        }

        private void SetupSqsUrl(SQSEvent.SQSMessage message)
        {
            var eventSourceARN = message.EventSourceArn;
            var queueName = Amazon.Arn.Parse(eventSourceARN).Resource;

            var senderId = message.Attributes.FirstOrDefault(a => a.Key.Equals(Constants.SENDER_ID)).Value;
            var awsRegion = message.AwsRegion;
            SqsUrl = $"https://sqs.{awsRegion}.amazonaws.com/{senderId}/{queueName}";
        }

        private void SetupReceipHandler(SQSEvent.SQSMessage message)
        {
            ReceiptHandle = message.ReceiptHandle;
        }

        /// <summary>
        /// This method changes MessageVisibility. 
        /// Here can be passed VisibilityTimeoutIncrement or NewVisibilityTimeout parameters that indicates 
        /// how MessageVisibility will be changed.
        /// </summary>
        /// <param name="previousDeliveries" type="int"></param>
        /// <param name="visibilityTimeoutIncrement" type="int"></param>
        /// <param name="newVisibilityTimeout" type="int"></param>
        /// 
        public async Task<ChangeMessageVisibilityResponse> ChangeMessageVisibility(int previousDeliveries, int visibilityTimeoutIncrement = 5, int newVisibilityTimeout = 0)
        {
            int visibilityTimeout = 0;

            if (newVisibilityTimeout != 0)
            {
                visibilityTimeout = newVisibilityTimeout;
            }

            if (visibilityTimeoutIncrement != 0)
            {
                var sqsAttributes = await GetAttributes();

                var visibilityTimeoutStr = sqsAttributes.FirstOrDefault(a => a.Key.Equals(Constants.VISIBILITY_TIMEOUT)).Value;
                _ = int.TryParse(visibilityTimeoutStr, out int oldVisibilityTimeout);

                //algorithm adds [visibilityTimeoutIncrement] seconds at every iteration to the queue.
                visibilityTimeout = oldVisibilityTimeout + visibilityTimeoutIncrement * (previousDeliveries - 1);
            }

            var response = await _sqsClient.ChangeMessageVisibilityAsync(SqsUrl, ReceiptHandle, visibilityTimeout);

            return response;
        }

        /// <summary>
        /// This method calls the DeleteMessageAsync method to delete the message from SQS.
        /// </summary>
        public async Task<DeleteMessageResponse> DeleteMessage()
        {
            var delRequest = new DeleteMessageRequest
            {
                QueueUrl = SqsUrl,
                ReceiptHandle = ReceiptHandle,
            };

            var deleteMessageResponse = await _sqsClient.DeleteMessageAsync(delRequest);

            return deleteMessageResponse;
        }

        /// <summary>
        /// This method calls the GetQueueAttributesAsync method to retrieve 
        /// the attributes of the SQS. 
        /// </summary>
        private async Task<Dictionary<string, string>> GetAttributes()
        {
            var attributes = new Dictionary<string, string>();

            var request = new GetQueueAttributesRequest
            {
                QueueUrl = SqsUrl,
                AttributeNames = SqsAttributesToRetrieve
            };

            var response = await _sqsClient.GetQueueAttributesAsync(request);

            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                foreach (var attr in response.Attributes)
                {
                    attributes.Add(attr.Key, attr.Value);
                }
            }

            return attributes;
        }

        public void Dispose()
        {

            if (_sqsClient != null)
            {
                _sqsClient.Dispose();
                _sqsClient = null;
            }
        }
    }
}
