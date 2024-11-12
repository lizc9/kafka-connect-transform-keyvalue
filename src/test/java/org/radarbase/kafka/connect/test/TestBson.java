package org.radarbase.kafka.connect.test;


import org.bson.json.JsonReader;
import org.junit.Test;
import org.radarbase.kafka.connect.utils.BsonDeserialization;

public class TestBson {
  private BsonDeserialization bsonDer = new BsonDeserialization();

  @Test
  public void testBsonDer() {
    String after = "{\"_id\": \"c-63185b0e-4d0b-3eb7-81ee-dc6219910207\",\"dataset_type\": \"h265_subrun_train\",\"update_time\": {\"$date\": 1730846828899}}";
    String oid = "{\"$oid\": \"672d320b8972041a6060895a\"}";
    String id = "{\"id\": \"672d320b8972041a6060895a\"}";
    String dateTime = "{\"$date\": \"2024-11-12T02:03:18.284Z\"}";
    String dateTimeArray = "[{\"$date\": \"2024-11-12T02:03:18.284Z\"}, {\"$date\": \"2024-11-12T08:03:18.284Z\"}]";
    String t = "672d320b8972041a6060895a";

//    JsonReader jsonReader = new JsonReader(dateTimeArray);
//    jsonReader.readStartArray();
  }
}
