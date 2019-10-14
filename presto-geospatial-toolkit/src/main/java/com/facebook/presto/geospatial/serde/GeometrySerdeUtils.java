package com.facebook.presto.geospatial.serde;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

class GeometrySerdeUtils
{
    static GeometrySerializationType readType(SliceInput input)
    {
        return GeometrySerializationType.getForCode(input.readByte());
    }

    static void writeType(SliceOutput output, GeometrySerializationType type)
    {
        output.writeByte(type.code());
    }

    static void skipEnvelope(SliceInput input)
    {
        int skipLength = 4 * SIZE_OF_DOUBLE;
        verify(input.skip(skipLength) == skipLength);
    }

    static double[] readCoordinates(SliceInput input)
    {
        int numPoints = input.readInt();
        double[] coordinates = new double[numPoints * 2];
        for (int i = 0; i < numPoints * 2; i++) {
            coordinates[i] = input.readDouble();
        }
        return coordinates;
    }
}

