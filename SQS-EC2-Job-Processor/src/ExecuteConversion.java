package de.tuhh.parallel.cw.jobprocessor;



import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.Topic;

/**
 * 
 * This is a simple class for executing a temp conversion.
 * Results are stored in simpleDB with unique Item ID
 * 
 * @author bjoern
 * 
 */
public class ExecuteConversion implements JobExecutor {

	private static Logger log = Logger.getLogger(ExecuteConversion.class);

	public String execute(String jobDefinition, String messageID) {
		String result = null;
		try {
			result = execute(jobDefinition);
			storeResult(jobDefinition, result, messageID);
		} catch (JobExecutionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	private void storeResult(String request, String result, String messageID)
			throws IOException {
        AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
                ExecuteConversion.class.getResourceAsStream("AwsCredentials.properties")));
        sdb.setEndpoint("https://sdb.eu-west-1.amazonaws.com");

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SimpleDB");
        System.out.println("===========================================\n");

		try {
            // Create a domain
            
            String myDomain = "Results";

            createDomain(myDomain,sdb);
            
         // Put data into a domain
            System.out.println("Putting data into " + myDomain + " domain.\n");
            sdb.batchPutAttributes(new BatchPutAttributesRequest(myDomain, formatData(messageID,request,result)));

            sendNotification(messageID);

		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SimpleDB, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SimpleDB, "
							+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public String execute(String jobDefinition) throws JobExecutionException {
		log.info("Executing job = '" + jobDefinition + "'");
		if (jobDefinition == null) {
			throw new JobExecutionException("Job definition is null");
		}
		if (jobDefinition.indexOf(' ') != -1) {
			throw new JobExecutionException("String has a space in it");
		}
		// business logic
		TemperatureConverter tc=new TemperatureConverter();
		String[] s=jobDefinition.split(" ");
		double value=Double.parseDouble(s[0]);
		double resultval=0;
		String resultunit="";
		if(s[1].equals("C") | s[1].equals("c")){
			resultval=tc.c2fConversion(value);
			resultunit="F";
		} else if(s[1].equals("F") | s[1].equals("f")){
			resultval=tc.f2cConversion(value);
			resultunit="C";
		}
		String meinString=resultval+" "+resultunit;

		return meinString;
	}

	private void sendNotification(String messageID) throws IOException {
		//Create a new SNS client  
		AmazonSNSClient sns=new AmazonSNSClient(new PropertiesCredentials(
		        ExecuteConversion.class.getResourceAsStream("AwsCredentials.properties")));
		
		//Set the endpoint for SNS to the European Region
		sns.setEndpoint("https://sns.eu-west-1.amazonaws.com");
		
		String etopicARN="arn:aws:sns:eu-west-1:283502715360:Temperature_Coverter_Delivery_Notification";
		String etopic_name="Temperature_Coverter_Delivery_Notification";
		String subject="Notification Message: "+messageID;
		
		//Create a notification message string
		String message="The conversion value with ID: "+messageID+", has been already stored into the database.";

		boolean flag=false;

		// List domains
        System.out.println("Listing all domains in your account:\n");
        for (Topic top : sns.listTopics().getTopics()) {
        	String topicARN=top.getTopicArn();
        	String[] s=topicARN.split(":");
            String name=s[5];
            if(name.equals(etopic_name)){
            	flag=true;
            	System.out.println("It is already created a Topic with ARN: " + topicARN+ ".\n");
            }
        }
        if(!flag){
        	System.out.println("Creating a topic with ARN " + etopicARN + ".\n");
        	
    		//Assign your Topic ARN from SNS to a string
            CreateTopicResult ctr=sns.createTopic(new CreateTopicRequest(etopic_name));
            etopicARN=ctr.getTopicArn();
        }
			


		//Create a publish request object
		PublishRequest preq= new PublishRequest(etopicARN,message,subject);

		//Publish your request
        sns.publish(preq);

	}
	
	
	/**
	 * Method used for creating a new Domain if it is the first time called.
	 * 
	 * @param myDomain
	 * @param sdb
	 */
	private void createDomain(String myDomain, AmazonSimpleDB sdb){
		boolean flag=false;

		// List domains
        System.out.println("Listing all domains in your account:\n");
        for (String domainName : sdb.listDomains().getDomainNames()) {
            
            if(domainName.equals(myDomain)){
            	flag=true;
            	System.out.println("It is already created a Domain" + domainName);
            }
        }
        if(!flag){
        	System.out.println("Creating domain called " + myDomain + ".\n");
            sdb.createDomain(new CreateDomainRequest(myDomain));	
        }
	}
	private List<ReplaceableItem> formatData(String id,String req,String conv){
		List<ReplaceableItem> data = new ArrayList<ReplaceableItem>();
        data.add(new ReplaceableItem("Item "+id).withAttributes(
                new ReplaceableAttribute("Id", id, true),
                new ReplaceableAttribute("Request", req, true),
        		new ReplaceableAttribute("Response", conv, true)));
        return data;
	}
}
