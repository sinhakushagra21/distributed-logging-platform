package com.observability.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Trivial deserializer that hands the raw Kafka value bytes straight through.
 * We deliberately defer Protobuf parsing to a ProcessFunction (LogParseFunction)
 * so that a malformed record can be routed to the dead-letter side output
 * instead of throwing inside the source (which would fail the whole job).
 */
public class BytesDeserializationSchema implements DeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] message) {
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}
