package org.radarbase.kafka.connect.utils;

import com.mongodb.internal.HexUtils;
import org.bson.*;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.JsonObjectCodec;
import org.bson.json.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/**
 * Default Mapping of Document is as below
 * more reference please check: com.ververica.cdc.connectors.mongodb.table.MongoDBConnectorDeserializationSchema
 *
 * DOCUMENT: org.bson.Document.class
 * ARRAY: java.util.List.class
 * DATE_TIME: java.util.Date.class
 * BOOLEAN: java.lang.Boolean.class
 * DOUBLE: java.lang.Double.class
 * INT32: java.lang.Integer.class
 * INT64: java.lang.Long.class
 * DECIMAL128: org.bson.types.Decimal128.class
 * STRING: java.lang.String.class
 * BINARY: org.bson.types.Binary.class
 * OBJECT_ID: org.bson.types.ObjectId.class
 * REGULAR_EXPRESSION: org.bson.types.RegularExpression.class
 * SYMBOL: org.bson.types.Symbol.class
 * DB_POINTER: org.bson.types.DBPointer.class
 * MAX_KEY: org.bson.types.MaxKey.class
 * MIN_KEY: org.bson.types.MinKey.class
 * JAVASCRIPT: org.bson.types.Code.class
 * JAVASCRIPT_WITH_SCOPE: org.bson.types.CodeWithScope.class
 * TIMESTAMP: org.bson.types.BSONTimestamp.class
 * UNDEFINED: org.bson.types.Undefined.class
 *
 */
public class BsonDeserialization {
    private transient JsonObjectCodec codec;

    private void initializeCodec() {
        this.codec = new JsonObjectCodec(JsonWriterSettings.builder()
                .outputMode(JsonMode.RELAXED)
                .objectIdConverter(this::convertObjectId)
                .decimal128Converter(this::convertBsonDecimal128)
                .binaryConverter(this::convertBsonBinary)
                .timestampConverter(this::convertBsonTimestamp)
                .dateTimeConverter(this::convertBsonDateTime)
                .undefinedConverter(this::convertBsonUndefined)
                .regularExpressionConverter(this::convertBsonRegularExpression)
                .javaScriptConverter(this::convertBsonJavascript)
                .symbolConverter(this::convertBsonSymbol)
                .int32Converter(this::int32Converter)
                .int64Converter(this::int64Converter)
                .doubleConverter(this::doubleConverter)
                .build());
    }

    /** convert BSON to JSON for special types using the method from Extended JSON */
    public JsonObject toJsonObject(BsonDocument bsonDocument) {
        if (codec == null) {
            initializeCodec();
        }
        DecoderContext decoderContext = DecoderContext.builder().build();
        return codec.decode(new BsonDocumentReader(bsonDocument), decoderContext);
    }

    public JsonObject toJsonObject(String bson) {
        if (codec == null) {
            initializeCodec();
        }
        DecoderContext decoderContext = DecoderContext.builder().build();
        return codec.decode(new JsonReader(bson), decoderContext);
    }


    private void convertObjectId(ObjectId objectId, StrictJsonWriter writer) {
        if (objectId == null) {
            writer.writeNull();
        } else {
            writer.writeString(objectId.toHexString());
        }
    }

    private void convertBsonDecimal128(Decimal128 decimal128, StrictJsonWriter writer) {
        if (decimal128 == null || decimal128.isNaN()) {
            writer.writeNull();
        } else {
            writer.writeString(decimal128.bigDecimalValue().toString());
        }
    }

    private void convertBsonBinary(BsonBinary bsonBinary, StrictJsonWriter writer) {
        if (BsonBinarySubType.isUuid(bsonBinary.getType())) {
            writer.writeString(bsonBinary.asUuid().toString());
        } else {
            writer.writeString(HexUtils.toHex(bsonBinary.getData()));
        }
    }

    private void convertBsonTimestamp(BsonTimestamp bsonTimestamp, StrictJsonWriter writer) {
        writer.writeNumber(String.valueOf(bsonTimestamp.getTime() * 1000L));
    }

    private void convertBsonDateTime(Long bsonDateTime, StrictJsonWriter writer) {
        writer.writeNumber(String.valueOf(bsonDateTime));
    }

    private void convertBsonUndefined(BsonUndefined bsonUndefined, StrictJsonWriter writer) {
        writer.writeNull();
    }

    private void convertBsonJavascript(String javascript, StrictJsonWriter writer) {
        writer.writeString(javascript);
    }

    private void convertBsonRegularExpression(BsonRegularExpression regex, StrictJsonWriter writer) {
        writer.writeString(String.format("/%s/%s", regex.getPattern(), regex.getOptions()));
    }

    private void convertBsonSymbol(final String symbol, final StrictJsonWriter writer) {
        writer.writeString(symbol);
    }

    private void int32Converter(Integer value, StrictJsonWriter writer) {
        if (value == null) {
            writer.writeNull();
        } else {
            writer.writeNumber(value.toString());
        }
    }

    private void int64Converter(Long value, StrictJsonWriter writer) {
        if (value == null) {
            writer.writeNull();
        } else {
            writer.writeNumber(value.toString());
        }
    }

    private void doubleConverter(Double value, StrictJsonWriter writer) {
        if (value == null || value.isNaN() || value.isInfinite()) {
            writer.writeNull();
        } else {
            writer.writeNumber(value.toString());
        }
    }

    public String convertBsonToJson(String bson) {
        if (bson != null && !bson.isEmpty()) {
            // parse string to bsonDoc
            BsonDocument bsonDoc = BsonDocument.parse(bson);
            // parse bson to json
            return toJsonObject(bsonDoc).getJson();
        } else {
            return null;
        }
    }
}
