package com.github.claudiodornelles.generic;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericRecordExamples {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericRecordExamples.class.getSimpleName());
  private static final Parser SCHEMA_PARSER = new Parser();
  private static final Schema CUSTOMER_SCHEMA = SCHEMA_PARSER.parse("{\n"
                                                                        + "    \"type\": \"record\",\n"
                                                                        + "    \"name\": \"Customer\",\n"
                                                                        + "    \"namespace\": \"com.github.claudiodornelles\",\n"
                                                                        + "    \"doc\": \"Avro Schema for a Customer\",\n"
                                                                        + "    \"fields\": [\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"first_name\",\n"
                                                                        + "            \"type\": \"string\",\n"
                                                                        + "            \"doc\": \"Customer's first name\"\n"
                                                                        + "        },\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"middle_name\",\n"
                                                                        + "            \"type\": [\"null\", \"string\"],\n"
                                                                        + "            \"default\": null,\n"
                                                                        + "            \"doc\": \"Customer's middle name\"\n"
                                                                        + "        },\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"last_name\",\n"
                                                                        + "            \"type\": \"string\",\n"
                                                                        + "            \"doc\": \"Customer's last name\"\n"
                                                                        + "        },\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"age\",\n"
                                                                        + "            \"type\": \"int\",\n"
                                                                        + "            \"doc\": \"Customer's age\"\n"
                                                                        + "        },\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"height\",\n"
                                                                        + "            \"type\": \"float\",\n"
                                                                        + "            \"doc\": \"Customer's height in centimeters\"\n"
                                                                        + "        },\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"weight\",\n"
                                                                        + "            \"type\": \"float\",\n"
                                                                        + "            \"doc\": \"Customer's weight in Kilograms\"\n"
                                                                        + "        },\n"
                                                                        + "        {\n"
                                                                        + "            \"name\": \"automated_email\",\n"
                                                                        + "            \"type\": \"boolean\",\n"
                                                                        + "            \"default\": true,\n"
                                                                        + "            \"doc\": \"true if the customer wants marketing emails\"\n"
                                                                        + "        }\n"
                                                                        + "    ]\n"
                                                                        + "}");

  public static void main(String[] args) {
    final var firstCustomerRecordBuilder = new GenericRecordBuilder(CUSTOMER_SCHEMA);
    firstCustomerRecordBuilder.set("first_name", "Gronheple");
    firstCustomerRecordBuilder.set("middle_name", "Aranlupo");
    firstCustomerRecordBuilder.set("last_name", "Valric");
    firstCustomerRecordBuilder.set("age", 56);
    firstCustomerRecordBuilder.set("height", 187.2);
    firstCustomerRecordBuilder.set("weight", 96.5);
    firstCustomerRecordBuilder.set("automated_email", false);
    final var firstCustomerRecord = firstCustomerRecordBuilder.build();
    LOGGER.info("firstCustomerRecord = {}", firstCustomerRecord);

    final var secondCustomerRecordBuilder = new GenericRecordBuilder(CUSTOMER_SCHEMA);
    secondCustomerRecordBuilder.set("first_name", "Adanthir");
    secondCustomerRecordBuilder.set("middle_name", "Reinduco");
    secondCustomerRecordBuilder.set("last_name", "Tuzloy");
    secondCustomerRecordBuilder.set("age", 37);
    secondCustomerRecordBuilder.set("height", 156.4);
    secondCustomerRecordBuilder.set("weight", 92.5);
    secondCustomerRecordBuilder.build();
    final var secondCustomerRecordWithDefaultValues = secondCustomerRecordBuilder.build();
    LOGGER.info("secondCustomerRecordWithDefaultValues = {}", secondCustomerRecordWithDefaultValues);

    // Serialize customers to disk
    final var datumWriter = new GenericDatumWriter<GenericRecord>(CUSTOMER_SCHEMA);
    final var file = new File("customers.avro");
    try (final var dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(CUSTOMER_SCHEMA, file);
      dataFileWriter.append(firstCustomerRecord);
      dataFileWriter.append(secondCustomerRecordWithDefaultValues);
    } catch (IOException e) {
      LOGGER.error("Could not write schema to file. {}", e.getMessage(), e);
    }

    // Deserialize customers from disk
    final var datumReader = new GenericDatumReader<GenericRecord>(CUSTOMER_SCHEMA);
    try {
      try (var dataFileReader = new DataFileReader<>(file, datumReader)) {
        GenericRecord customer = null;
        while (dataFileReader.hasNext()) {
          customer = dataFileReader.next(customer);
          LOGGER.info("Read customer from file: {}", customer);
          LOGGER.info("customer.getSchema(): {}", customer.getSchema());
          LOGGER.info("customer.get(\"first_name\"): {}", customer.get("first_name"));
        }
      }
    } catch (IOException e) {
      LOGGER.error("Could not read schema from file. {}", e.getMessage(), e);
    }
  }

}
