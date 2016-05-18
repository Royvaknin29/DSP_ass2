package local_application;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.google.common.collect.Lists;

import ass1.amazon_utils.Ec2InstanceType;
import ass1.amazon_utils.SQSservice;

public class RunLocalApplication {
	private static String accKey = "AKIAJQATFWOYRXSG5XGQ";
	private static String secKey = "BTu0f7xOSqJ0SgtGXhhDcs+6sa89y/vSr/ZAo1xi";

	public static void main(String[] args) {
		AWSCredentials credentials = setCredentialsFromArgs(accKey, secKey);
		if (args.length < 3) {
			System.out.println("Missing arguments!\naborting...");
			System.exit(1);
		}
		if (args.length == 4) {
			if (args[3].equals("terminate")) {
				System.out.println("Terminating..");
				AmazonEC2Client ec2Client = new AmazonEC2Client(credentials);
				TerminateInstancesRequest termination = new TerminateInstancesRequest(getAllRunningInstances(ec2Client));
				ec2Client.terminateInstances(termination);
				System.exit(0);
			}
		}
		LocalApplication localApplication = new LocalApplication(credentials, args[1]);
		try {
			localApplication.startApplication(args);
		} catch (Exception e) {
			System.out.println("Exception in local appliation.." + e);
		}
	}

	public static List<String> getAllRunningInstances(AmazonEC2Client ec2Client) {
		List<String> ids = Lists.newArrayList();
		List<Reservation> reservations = ec2Client.describeInstances().getReservations();
		List<Instance> instances = null;
		for (Reservation resv : reservations) {
			instances = resv.getInstances();
			for (Instance instance : instances) {
				ids.add(instance.getInstanceId());
			}
		}
		return ids;
	}

	public static AWSCredentials setCredentialsFromArgs(String accKey, String seckey) {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials(accKey, seckey);
		} catch (Exception e) {
			throw new AmazonClientException("credentials given fail to log ...", e);
		}
		return credentials;

	}
}
