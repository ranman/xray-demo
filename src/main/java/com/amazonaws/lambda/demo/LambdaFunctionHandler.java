package com.amazonaws.lambda.demo;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;

public class LambdaFunctionHandler implements RequestHandler<S3Event, Object> {
    private static final AmazonRekognition rekogClient;
    private static final AmazonDynamoDB ddbClient;
    private static final String TABLE_NAME = System.getenv("DDB_TABLE");
    static {
        rekogClient = AmazonRekognitionClientBuilder.defaultClient();
        ddbClient = AmazonDynamoDBClientBuilder.defaultClient();
    }

    public Map<String, AttributeValue> getImageLabels(String bucket, String key) {
        DetectLabelsRequest request = new DetectLabelsRequest()
                .withImage(new Image().withS3Object(new S3Object().withName(key).withBucket(bucket)))
                .withMaxLabels(50)
                .withMinConfidence(70F);
        Map<String, AttributeValue> labelMap = new HashMap<String, AttributeValue>();
        try {
            for (Label label: rekogClient.detectLabels(request).getLabels()) {
                labelMap.put(label.getName(), new AttributeValue().withN(label.getConfidence().toString()));
            }
        } catch(AmazonRekognitionException ex) {
            throw ex;
        }
        return labelMap;
    }

    public Object handleRequest(S3Event input, Context context) {
        // We're only going to get one object so just grab that first one
        S3Entity s3obj = input.getRecords().get(0).getS3();
        String bucket = s3obj.getBucket().getName();
        String key = s3obj.getObject().getKey();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        Map<String, AttributeValue> labelMap = getImageLabels(bucket, key);
        if (labelMap.isEmpty()) {
            return null;
        }
        item.put("id", new AttributeValue().withS(key));
        item.put("labels", new AttributeValue().withM(labelMap));
        ddbClient.putItem(TABLE_NAME, item);
        return null;
    }
}
