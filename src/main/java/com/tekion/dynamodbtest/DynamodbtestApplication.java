package com.tekion.dynamodbtest;

//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
//import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
//import com.amazonaws.services.dynamodbv2.document.DynamoDB;
//import com.amazonaws.services.dynamodbv2.document.Item;
//import com.amazonaws.services.dynamodbv2.document.Table;
//import com.fasterxml.jackson.core.JsonFactory;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import java.io.IOException;
//import java.util.Iterator;
//import jdk.nashorn.internal.ir.ObjectNode;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.json.JsonParser;
import java.io.File;
import java.util.Iterator;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.amazonaws.regions.Regions;
import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DynamodbtestApplication {

  private static final AWSCredentials AWS_CREDENTIALS;
  static {
    // Your accesskey and secretkey
    AWS_CREDENTIALS = new BasicAWSCredentials(
        "***************",
        "***********************"
    );
  }
  public static void main(String[] args) throws IOException {
    SpringApplication.run(DynamodbtestApplication.class, args);

    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
        .withRegion(Regions.US_WEST_1)
        .build();

    DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable("tst_Employee_Test");
    JsonParser parser = new JsonFactory().createParser(new File("/Users/sandhyasingh/Downloads/convertcsv.json"));
    JsonNode rootNode = new ObjectMapper().readTree(parser);
    Iterator<JsonNode> iter = rootNode.iterator();
    ObjectNode currentNode;
    while (iter.hasNext()) {
      currentNode = (ObjectNode) iter.next();
      String name = currentNode.path("Name").asText();
      String jobTitles = currentNode.path("Job Titles").asText();
      String department=currentNode.path("Department").asText();
      String fullOrParttime= currentNode.path("Full or Part-Time").asText();
      String salaryOrHourly= currentNode.path("Salary or Hourly").asText();
      String  annualSalary =currentNode.path("Annual Salary").asText();
      String hourlyRate = currentNode.path("Hourly Rate").asText();
      try {
        table.putItem(new Item().withPrimaryKey("Name", name, "Job Titles", jobTitles).withJSON("department",
            currentNode.path("Department").toString()).withJSON("fullOrParttime",currentNode.path("Full or Part-Time").toString()).withJSON
            ("salaryOrHourly",currentNode.path("Salary or Hourly").toString())
            .withJSON("annualSalary",currentNode.path("Annual Salary").toString())
            .withJSON("hourlyRate",currentNode.path("Hourly Rate").toString()));


//        table.putItem(new Item().withPrimaryKey("Name", name, "Job Titles", jobTitles).withJSON("department",
//            currentNode.path("Department").toString(),"fullOrParttime",currentNode.path("Full or Part-Time").toString(),
//            "salaryOrHourly",currentNode.path("Salary or Hourly").toString(),"annualSalary",currentNode.path("Annual Salary"),
//            "hourlyRate",currentNode.path("Hourly Rate").toString()));
      }
      catch (Exception e) {
        System.err.println("Unable to add");
        System.err.println(e.getMessage());
        break;
      }
    }
    parser.close();
  }


}
