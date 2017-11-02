package com.github.davidmoten.rx2.aws;

import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.aws.AwsClients;

public class AwsClientsTest {

	@Test
	public void tests3() {
		AwsClients.createS3Client(new AWSCredentials() {

			@Override
			public String getAWSSecretKey() {
				return "a";
			}

			@Override
			public String getAWSAccessKeyId() {
				return "b";
			}
		}, new ClientConfiguration(), Regions.AP_SOUTHEAST_2);
	}
	
	@Test
	public void testSqs() {
		AwsClients.createSqsClient(new AWSCredentials() {

			@Override
			public String getAWSSecretKey() {
				return "a";
			}

			@Override
			public String getAWSAccessKeyId() {
				return "b";
			}
		}, new ClientConfiguration(), Regions.AP_SOUTHEAST_2);
	}
	
	@Test
	public void isUtilityClass() {
		Asserts.assertIsUtilityClass(AwsClients.class);
	}
}
