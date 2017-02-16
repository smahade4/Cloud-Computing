import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class AmazonDynamoDB {

    static AmazonDynamoDBClient dynamoDB;
    String tableName = "checkDuplicate";
	
	public  void init() throws Exception {
    	BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAIKJ7XKFQAKDOLAZA", "liQQanhZzwZu5NZZjATHdm4SYfotiR+118Ulgcdt");
    	dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
try
{
        if (Tables.doesTableExist(dynamoDB, tableName)) {
       //     System.out.println("Table: " + tableName + " is already ACTIVE");
        } else {
            // Create a table with a primary hash key named 'name', which holds a string
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(2L).withWriteCapacityUnits(2L));
                TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
            System.out.println("Created Table: " + createdTableDescription);

            // Wait for it to become active
            System.out.println("Waiting for " + tableName + " to become ACTIVE...");
            Tables.waitForTableToBecomeActive(dynamoDB, tableName);
        }
}
catch(ResourceInUseException e)
{
    Tables.waitForTableToBecomeActive(dynamoDB, tableName);
	
}
	}
	
	

    public int putddb(String taskid) throws Exception {
        
        try {

            // Create table if it does not exist yet

            Map<String, AttributeValue> item = newItem(taskid);
         //  System.out.println("inside process of putdb"); 
           
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            try{ PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            }catch( ConditionalCheckFailedException e)
            {
         	   
                return 0;          
            }
       
        }
            catch(Exception e)
            {
            	
            }
        return 1;     
        
        }
    private static Map<String, AttributeValue> newItem(String name) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(name));
   
        return item;
    }
}
