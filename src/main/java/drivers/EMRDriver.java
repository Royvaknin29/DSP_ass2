package drivers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
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

		AWSCredentials credentials = null;
		try {
			AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
			// credentials = new PropertiesCredentials(
			credentials = credentialsProvider.getCredentials();
			AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);

			HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
					.withJar("https://s3.amazonaws.com/roy-aaron-dsp-ass2/Ass_2.jar")
					.withMainClass("drivers.WordCountTest")
					.withArgs("s3n://https://s3.amazonaws.com/roy-aaron-dsp-ass2/input/",
							"https://s3.amazonaws.com/roy-aaron-dsp-ass2/output/");

			StepConfig stepConfig = new StepConfig().withName("stepname").withHadoopJarStep(hadoopJarStep)
					.withActionOnFailure("TERMINATE_JOB_FLOW");

			JobFlowInstancesConfig instances = new JobFlowInstancesConfig().withInstanceCount(2)
					.withMasterInstanceType(InstanceType.M1Small.toString())
					.withSlaveInstanceType(InstanceType.M1Small.toString()).withHadoopVersion("2.7.1")
					.withEc2KeyName("first_key_pair").withKeepJobFlowAliveWhenNoSteps(false)
					.withPlacement(new PlacementType("us-east-1a"));

			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest().withName("jobname").withInstances(instances)
					.withSteps(stepConfig).withLogUri("https://s3.amazonaws.com/roy-aaron-dsp-ass2/logs/");

			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		} catch (Exception e) {
			throw new AmazonClientException("credentials given fail to log ...", e);
		}
	}
}
