package ass1.amazon_utils;

import static org.apache.commons.codec.binary.Base64.encodeBase64;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.ShutdownBehavior;
import com.amazonaws.services.ec2.model.Tag;
import com.google.common.collect.Lists;

public class EC2LaunchFactory {
	private static String KEY_NAME = "dsp162";
	private static String SECURITY_GROUP = "launch-wizard-1";
	private static String INSTANCE_TYPE = "t2.micro";
	private AmazonEC2Client EC2Client;

	public EC2LaunchFactory(AWSCredentials credentials) {
		AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
		amazonEC2Client.setEndpoint("ec2.us-east-1.amazonaws.com");
		this.EC2Client = amazonEC2Client;
	}

	public void launchEC2Instance(Ec2InstanceType type){
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
		runInstancesRequest
				.setInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);
		runInstancesRequest.withImageId("ami-0c5fb961")
				.withInstanceType(INSTANCE_TYPE).withMinCount(1)
				.withMaxCount(1).withKeyName(KEY_NAME)
				.withSecurityGroups(SECURITY_GROUP)
				.setUserData(generateShellExtractionCommand(type));
		RunInstancesResult runInstancesResult = this.EC2Client
				.runInstances(runInstancesRequest);
		String instanceId = runInstancesResult.getReservation().getInstances()
				.get(0).getInstanceId();
		System.out.println("Created instanceID:" + instanceId + "Of Type:"
				+ type.name());
		CreateTagsRequest tagRequest = createTagRequest("Type", type.name());
		tagRequest.withResources(instanceId);
		try {Thread.sleep(1000);}
		catch (InterruptedException e) {e.printStackTrace();}
		this.EC2Client.createTags(tagRequest);
	}

	public static String generateShellExtractionCommand(Ec2InstanceType type) {
		if (type.equals(Ec2InstanceType.MANAGER)){
		return new String(
				encodeBase64("#!/bin/bash\ncd ~\nwget \"https://s3.amazonaws.com/jars-roy-aaron/Manager.zip\"\nunzip -P `cat ./pass` Manager\njava -jar Manager.jar > log"
						.getBytes()));
		}
		else{
			return new String(
					encodeBase64("#!/bin/bash\ncd ~\nwget \"https://s3.amazonaws.com/jars-roy-aaron/Worker.zip\"\nunzip -P `cat ./pass` Worker\njava -jar -Xms128m -Xmx768m Worker.jar  > log"
							.getBytes()));
		}
	}
	

	public static CreateTagsRequest createTagRequest(String key, String value) {
		Tag t = new Tag();
		t.setKey(key);
		t.setValue(value);
		List<Tag> tags = Lists.newArrayList(t);
		CreateTagsRequest ctr = new CreateTagsRequest();
		ctr.setTags(tags);
		return ctr;
	}
}