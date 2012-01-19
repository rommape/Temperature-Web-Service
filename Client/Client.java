package de.tuhh.parallel.cw.client;


import java.util.List;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;


public class Client {

	/**
	 * Class that implements the simple client that tests the TemperatureConverter methods.
	 * 
	 * @author Román Masanés Martínez
	 * @version 10.1.2012
	 */
	public static void main(String[] args) {
		
		String queueURL="https://queue.amazonaws.com:443/MyQueue4Requests";
		String myDomain="Results";

        try{
        	if(args==null || args.length!=2){
        		new Exception("Malformed arguments");
        	}

            AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
                    Client.class.getResourceAsStream("AwsCredentials.properties")));
            sdb.setEndpoint("https://sdb.eu-west-1.amazonaws.com");
            System.out.println("===========================================");
            System.out.println("Getting Started with Amazon SimpleDB");
            System.out.println("===========================================\n");
            AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(
                    Client.class.getResourceAsStream("AwsCredentials.properties")));
            sqs.setEndpoint("https://sqs.eu-west-1.amazonaws.com");

            System.out.println("===========================================");
            System.out.println("Getting Started with Amazon SQS");
            System.out.println("===========================================\n");
    		

			//process arguments
            Base64 b64= new Base64();
        	String messageBody=args[0]+":"+args[1];
        	String unit;
        	if(args[1].equals("c")||args[1].equals("C")){
        		unit="F";
        	}else if(args[1].equals("f")||args[1].equals("F")){
        		unit="C";
        	}else if(args[1].equals("d")||args[1].equals("D")){
        		sqs.deleteQueue(new DeleteQueueRequest(queueURL));
        		System.out.println("Queue deleted.\n");
        		return;
        	}else{
        		throw new Exception("Unit not recognized");
        	}
        	messageBody=b64.encode(messageBody);
			//send message to the Queue
            System.out.println("Sending a message to MyQueue.\n");
            SendMessageRequest message=new SendMessageRequest(queueURL, messageBody);
            SendMessageResult result;
            String messageID="";
            result=sqs.sendMessage(message);
            messageID=result.getMessageId();

			//look for the result on the Domain

            boolean flag=false;
            for (String domainName : sdb.listDomains().getDomainNames()) {
                
                if(domainName.equals(myDomain)){
                	flag=true;
                	System.out.println("It is already created a Domain" + domainName);
                }
            }
            if(!flag){
            	throw new Exception("The database is not available");	
            } else {
    			//present the data
            	System.out.println("Waiting 10 seconds for the response.\n");
            	Thread.sleep(10000);
                String selectExpression = "select * from `" + myDomain + "` where Id = '"+messageID+"'";
                System.out.println("Selecting: " + selectExpression + "\n");
                SelectRequest selectRequest = new SelectRequest(selectExpression,true);
                List<Item> items=sdb.select(selectRequest).getItems();
                if(items.isEmpty()){
                	System.out.println("No conversion yet.");
                }else{
	                for (Item item : items) {
	                    for (Attribute attribute : item.getAttributes()) {
	                        System.out.println(attribute.getName()+" : "+attribute.getValue());
	                    }
	                }
                }
            }


        }
        catch(Exception e){
        	System.out.println("Error Message: " + e.getMessage());
        }

	}

}
