using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Textract.Model;
using Amazon.Textract;
using Amazon;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWS_S3_Lambda_Trigger;

public class Function
{
    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client"></param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }
    
    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task<string?> FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var s3Event = evnt.Records?[0].S3;
        if(s3Event == null)
        {
            return null;
        }

        try
        {
            var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
            var file = await this.S3Client.GetObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key);
            using var reader = new StreamReader(file.ResponseStream);
            var fileContents = await reader.ReadToEndAsync();

            context.Logger.LogInformation($"Bucket Name: {file.BucketName}");
            context.Logger.LogInformation($"File Name: {file.Key}");

            using (var textractClient = new AmazonTextractClient(RegionEndpoint.APSouth1))
            {
                using (var s3Client = new AmazonS3Client(RegionEndpoint.APSouth1))
                {
                    context.Logger.LogInformation("Start document detection job");

                    var startJobRequest = new StartDocumentTextDetectionRequest
                    {
                        DocumentLocation = new DocumentLocation
                        {
                            S3Object = new Amazon.Textract.Model.S3Object
                            {
                                Bucket = file.BucketName,
                                Name = file.Key
                            }
                        }
                    };
                    var startJobResponse = await textractClient.StartDocumentTextDetectionAsync(startJobRequest);

                    context.Logger.LogInformation($"Job ID: {startJobResponse.JobId}");

                    var getDetectionRequest = new GetDocumentTextDetectionRequest
                    {
                        JobId = startJobResponse.JobId
                    };

                    context.Logger.LogInformation("Poll for detect job to complete");
                    // Poll till job is no longer in progress.
                    GetDocumentTextDetectionResponse getDetectionResponse;
                    do
                    {
                        Thread.Sleep(1000);
                        getDetectionResponse = await textractClient.GetDocumentTextDetectionAsync(getDetectionRequest);
                    } 
                    while (getDetectionResponse.JobStatus == JobStatus.IN_PROGRESS);

                    context.Logger.LogInformation("Print out results if the job was successful.");
                    // If the job was successful loop through the pages of results and print the detected text
                    if (getDetectionResponse.JobStatus == JobStatus.SUCCEEDED)
                    {
                        do
                        {
                            foreach (var block in getDetectionResponse.Blocks)
                            {
                                context.Logger.LogInformation($"Type {block.BlockType}, Text: {block.Text}");
                            }

                            // Check to see if there are no more pages of data. If no then break.
                            if (string.IsNullOrEmpty(getDetectionResponse.NextToken))
                            {
                                break;
                            }

                            getDetectionRequest.NextToken = getDetectionResponse.NextToken;
                            getDetectionResponse = await textractClient.GetDocumentTextDetectionAsync(getDetectionRequest);

                        } while (!string.IsNullOrEmpty(getDetectionResponse.NextToken));
                    }
                    else
                    {
                        context.Logger.LogInformation($"Job failed with message: {getDetectionResponse.StatusMessage}");
                    }
                }
            }
            context.Logger.LogInformation("Successfully executed");
            return response.Headers.ContentType;
        }
        catch(Exception e)
        {
            context.Logger.LogInformation($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
            context.Logger.LogInformation(e.Message);
            context.Logger.LogInformation(e.StackTrace);
            throw;
        }
    }
}