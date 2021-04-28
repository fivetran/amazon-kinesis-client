package com.fivetran.external.com.amazonaws.services.testing;

import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class DynamoTestDbSpec {

    private static final String DEBUG_TABLE = "benv_worker_debug";

    @Before
    public void before() {
        if (DynamoTestUtils.tableExists(DEBUG_TABLE)) DynamoTestUtils.deleteTable(DEBUG_TABLE);
    }

    @AfterClass
    public static void afterClass() {
        if (DynamoTestUtils.tableExists(DEBUG_TABLE)) DynamoTestUtils.deleteTable(DEBUG_TABLE);
    }

    @Test
    public void createTable() {
        // arrange
        String tableName = DEBUG_TABLE;
        String pkeyName = "pkey";

        KeySchemaElement pkeySchemaElement = new KeySchemaElement().withAttributeName(pkeyName).withKeyType(KeyType.HASH);
        List<KeySchemaElement> keySchema = ImmutableList.of(pkeySchemaElement);

        AttributeDefinition pkeyAttribute = new AttributeDefinition()
                .withAttributeName(pkeyName)
                .withAttributeType(ScalarAttributeType.S);

        List<AttributeDefinition> attributes = ImmutableList.of(pkeyAttribute);

        // act
        DynamoTestUtils.createTable(tableName, false, keySchema, attributes);

        // assert
        assert DynamoTestUtils.tableExists(tableName);
    }

    @Test
    public void createSimpleTestTable() {
        boolean created = DynamoTestUtils.createSimpleTable(DEBUG_TABLE);
        assert created == DynamoTestUtils.tableExists(DEBUG_TABLE);
    }

    @Test
    public void fillSimpleTable() {
        Long numRows = 24L;
        DynamoTestUtils.createSimpleTable(DEBUG_TABLE);
        DynamoTestUtils.fillSimpleTable(DEBUG_TABLE, numRows);
        // you have to check manually, describeTable doesnt update in a reasonable amount of time
    }

}
