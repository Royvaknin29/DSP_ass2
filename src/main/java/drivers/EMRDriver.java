package drivers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class EMRDriver {

	private static String accKey = "";
	private static String secKey = "";

	public static void main(String[] args) {

//		AWSCredentials credentials = null;
		try {
//			AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
//			credentials = new PropertiesCredentials(
//			credentials = credentialsProvider.getCredentials();
			AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
			System.out.println("credentials are set!");
			AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
			System.out.println("successfully connected to emr!");

			HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig().withJar("s3n://roy-aaron-dsp-ass2/Ass_2.jar")
					.withMainClass("drivers.WordCountTest")
					.withArgs("s3n://roy-aaron-dsp-ass2/input", "s3n://roy-aaron-dsp-ass2/output");

			StepConfig stepConfig = new StepConfig().withName("stepname").withHadoopJarStep(hadoopJarStep)
					.withActionOnFailure("TERMINATE_JOB_FLOW");

			JobFlowInstancesConfig instances = new JobFlowInstancesConfig().withInstanceCount(2)
					.withMasterInstanceType(InstanceType.M3Xlarge.toString())
					.withSlaveInstanceType(InstanceType.M3Xlarge.toString()).withHadoopVersion("2.7.2")
					.withEc2KeyName("first_key_pair").withKeepJobFlowAliveWhenNoSteps(false)
					.withPlacement(new PlacementType("us-east-1a"));

			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest().withName("jobname").withInstances(instances)
					.withSteps(stepConfig).withLogUri("s3n://roy-aaron-dsp-ass2/logs");

			runFlowRequest.setServiceRole("EMR_DefaultRole");
			runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");

			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		} catch (Exception e) {
			throw new AmazonClientException("credentials given fail to log ...", e);
		}
	}

	public static AWSCredentials setCredentialsFromArgs(String accKey,
														String seckey) {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accKey, seckey);
		} catch (Exception e) {
			throw new AmazonClientException(
					"credentials given fail to log ...", e);
		}
		return credentials;

	}
}
