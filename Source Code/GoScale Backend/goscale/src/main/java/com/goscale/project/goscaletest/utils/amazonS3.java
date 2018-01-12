package com.goscale.project.goscaletest.utils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

public class amazonS3 {
	private AmazonS3 amazonS3;
	private static final String SUFFIX = "/";

	public amazonS3() {
		amazonS3 = new AmazonS3Client(new BasicAWSCredentials(Constants.accessKeyS3, Constants.secretkey));
	}
	
	public String storeDataInS3(String key, InputStream inputStream, long length) {
		ObjectMetadata metadata = new ObjectMetadata();
		String strBucketName = Constants.bucketS3;
		
		 // metadata.addUserMetadata("type", "image");
		metadata.setContentLength(length);
		//System.out.print("length is " + length +"\n");
		// url and thubnail
		PutObjectRequest por = new PutObjectRequest(strBucketName, key, inputStream, metadata);
		por.setCannedAcl(CannedAccessControlList.PublicRead);
		PutObjectResult result = amazonS3.putObject(por);
		return result.getETag();
	}

	public void createFolder(String folderName) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(Constants.bucketS3,
					folderName + SUFFIX, emptyContent, metadata);
		// send request to S3 to create folder
		amazonS3.putObject(putObjectRequest);
	}
	
	public void removeData(String key) {
		amazonS3.deleteObject(Constants.bucketS3, key);
	}
	
	public void deleteFolder(String folderName, AmazonS3 client) {
		String bucketName = Constants.bucketS3;
		amazonS3.deleteObject(bucketName, folderName);
	}
}
