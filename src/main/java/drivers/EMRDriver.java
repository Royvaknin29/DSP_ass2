package drivers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
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

	private static String accKey = "AKIAJ45RMQKCWPDSND2A";
	private static String secKey = "aQbLwB5Lj9l1O3JVAlkJK1BmPCtEYZGCBK0v3cig";

	public static void main(String[] args) {

		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accKey, secKey);
		} catch (Exception e) {
			throw new AmazonClientException("credentials given fail to log ...", e);
		}
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig().withJar("s3n://yourbucket/yourfile.jar")
				.withMainClass("some.pack.MainClass").withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");

		StepConfig stepConfig = new StepConfig().withName("stepname").withHadoopJarStep(hadoopJarStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig().withInstanceCount(2)
				.withMasterInstanceType(InstanceType.M1Small.toString())
				.withSlaveInstanceType(InstanceType.M1Small.toString()).withHadoopVersion("2.2.0")
				.withEc2KeyName("yourkey").withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest().withName("jobname").withInstances(instances)
				.withSteps(stepConfig).withLogUri("s3n://yourbucket/logs/");

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
