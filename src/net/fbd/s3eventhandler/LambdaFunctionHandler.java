package net.fbd.s3eventhandler;

import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoder;
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderClient;
import com.amazonaws.services.elastictranscoder.model.CreateJobOutput;
import com.amazonaws.services.elastictranscoder.model.CreateJobRequest;
import com.amazonaws.services.elastictranscoder.model.CreateJobResult;
import com.amazonaws.services.elastictranscoder.model.JobInput;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> 
{

	// Private Variables
	private static final String ET_PIPELINE_ID = "1440334510935-skvork";
	private static final String ET_PRESET_ID = "1351620000001-100070";

	@Override
	public String handleRequest(S3Event input, Context context) 
	{
		try 
		{
			// Logger
			LambdaLogger logger = context.getLogger();

			// Get Event Record
			S3EventNotificationRecord record = input.getRecords().get(0);

			// Source File Name
			String srcFileName = record.getS3().getObject().getKey(); // Name doesn't contain any special characters

			// Destination File Name
			String distFileName = srcFileName + ".mp4";

			// ET Client
			AmazonElasticTranscoder transcoderClient = new AmazonElasticTranscoderClient();

			// ET Job Request
			CreateJobRequest createJobRequest = new CreateJobRequest();
			createJobRequest.setPipelineId(ET_PIPELINE_ID);
			
			// ET Job Input
			JobInput jobInput = new JobInput();
			jobInput.setKey(srcFileName);
									
			// ET Job Output
			CreateJobOutput createJobOutput = new CreateJobOutput();
			createJobOutput.setPresetId(ET_PRESET_ID);
			createJobOutput.setKey(distFileName);
			
			// Create ET Job
			createJobRequest.setInput(jobInput);
			createJobRequest.setOutput(createJobOutput);
			CreateJobResult result = transcoderClient.createJob(createJobRequest);
			
			//LOG
			logger.log("Job Created: " + result.getJob().toString());
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}

		return "OK";
	}
}
