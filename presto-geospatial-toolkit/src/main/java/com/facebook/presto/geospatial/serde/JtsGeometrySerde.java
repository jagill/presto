/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.geospatial.serde;

import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.serde.GeometrySerde.skipEnvelope;
import static com.facebook.presto.geospatial.serde.GeometrySerde.writeType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * We serialize JTS geometries similar to ESRI geometries.
 * @see com.facebook.presto.geospatial.serde.GeometrySerde
 */
public class JtsGeometrySerde
{
    // TODO: Are we sure this is thread safe?
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private JtsGeometrySerde() {}

    /**
     * Serialize JTS {@link Geometry} shape into an ESRI shape
     */
    public static Slice serialize(Geometry geometry)
    {
        requireNonNull(geometry, "input is null");
        DynamicSliceOutput output = new DynamicSliceOutput(100);
        GeometrySerializationType type = GeometrySerializationType.of(geometry);
        writeType(output, type);
        if (type != GeometrySerializationType.POINT) {
            writeEnvelopeCoordinates(output, geometry.getEnvelopeInternal());
        }
        writeGeometry(output, geometry);
        return output.slice();
    }

    public static Slice serialize(Envelope envelope)
    {
        requireNonNull(envelope, "envelope is null");
        verify(!envelope.isNull());
        DynamicSliceOutput output = new DynamicSliceOutput(33);
        writeType(output, GeometrySerializationType.ENVELOPE);
        writeEnvelopeCoordinates(output, envelope);
        return output.slice();
    }

    private static void writeEnvelopeCoordinates(SliceOutput output, Envelope envelope)
    {
        if (envelope.isNull()) {
            output.appendDouble(NaN);
            output.appendDouble(NaN);
            output.appendDouble(NaN);
            output.appendDouble(NaN);
        }
        else {
            output.appendDouble(envelope.getMinX());
            output.appendDouble(envelope.getMinY());
            output.appendDouble(envelope.getMaxX());
            output.appendDouble(envelope.getMaxY());
        }
    }

    private static void writeGeometry(SliceOutput output, Geometry geometry)
    {
        switch (GeometryType.getForJtsGeometryType(geometry.getGeometryType())) {
            case POINT:
                writePoint(output, (Point) geometry);
                break;
            case MULTI_POINT:
            case LINE_STRING:
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
                writeSimpleGeometry(output, geometry);
                break;
            case GEOMETRY_COLLECTION:
                verify(geometry instanceof GeometryCollection);
                writeGeometryCollection(output, (GeometryCollection) geometry);
                break;
            default:
                throw new IllegalArgumentException("Unsupported geometry type : " + geometry.getGeometryType());
        }
    }

    private static void writePoint(SliceOutput output, Point point)
    {
        if (!point.isEmpty()) {
            Coordinate coordinate = point.getCoordinate();
            output.writeDouble(coordinate.x);
            output.writeDouble(coordinate.y);
        }
        else {
            output.writeDouble(NaN);
            output.writeDouble(NaN);
        }
    }

    private static void writeSimpleGeometry(SliceOutput output, Geometry geometry)
    {
        output.appendBytes(new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geometry));
    }

    private static void writeGeometryCollection(SliceOutput output, GeometryCollection collection)
    {
        int numGeometries = collection.getNumGeometries();
        output.appendInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            Geometry geometry = collection.getGeometryN(geometryIndex);
            int startPosition = output.size();
            // leave 4 bytes for the shape length
            output.appendInt(0);

            // Write the Geometry
            writeType(output, GeometrySerializationType.of(geometry));
            writeGeometry(output, geometry);

            // Retroactively write the geometry length
            int endPosition = output.size();
            int length = endPosition - startPosition - Integer.BYTES;
            output.getUnderlyingSlice().setInt(startPosition, length);
        }
    }

    public static Geometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
        if (type != GeometrySerializationType.POINT && type != GeometrySerializationType.ENVELOPE) {
            skipEnvelope(input);
        }
        return readGeometry(input, shape, type, input.available());
    }

    private static Geometry readGeometry(BasicSliceInput input, Slice shape, GeometrySerializationType type, int length)
    {
        switch (type) {
            case POINT:
                return readPoint(input);
            case MULTI_POINT:
            case LINE_STRING:
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
                return readSimpleGeometry(input, shape, length);
            case GEOMETRY_COLLECTION:
                return readGeometryCollection(input, shape);
            case ENVELOPE:
                return createPolygonFromEnvelope(readEnvelope(input));
            default:
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected type in deserialization: " + type);
        }
    }

    private static Point readPoint(BasicSliceInput input)
    {
        Coordinate coordinates = readCoordinate(input);
        if (isNaN(coordinates.x) || isNaN(coordinates.y)) {
            return GEOMETRY_FACTORY.createPoint();
        }
        return GEOMETRY_FACTORY.createPoint(coordinates);
    }

    private static Geometry readSimpleGeometry(BasicSliceInput input, Slice shape, int length)
    {
        byte[] geometryBytes = shape.getBytes(toIntExact(input.position()), length);
        input.skip(length);
        try {
            return new WKBReader(GEOMETRY_FACTORY).read(geometryBytes);
        }
        catch (ParseException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static GeometryCollection readGeometryCollection(BasicSliceInput input, Slice inputSlice)
    {
        int numGeometries = input.readInt();
        List<Geometry> geometries = new ArrayList<>(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            int length = input.readInt();
            GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
            geometries.add(readGeometry(input, inputSlice, type, length - 1));
        }
        return GEOMETRY_FACTORY.createGeometryCollection(geometries.toArray(new Geometry[0]));
    }

    private static Geometry createPolygonFromEnvelope(Envelope envelope)
    {
        requireNonNull(envelope, "envelope is null");
        if (envelope.isNull()) {
            // Empty envelope, empty geometry
            return GEOMETRY_FACTORY.createPolygon();
        }

        double xMin = envelope.getMinX();
        double yMin = envelope.getMinY();
        double xMax = envelope.getMaxX();
        double yMax = envelope.getMaxY();

        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(xMin, yMin);
        coordinates[1] = new Coordinate(xMax, yMin);
        coordinates[2] = new Coordinate(xMax, yMax);
        coordinates[3] = new Coordinate(xMin, yMax);
        coordinates[4] = coordinates[0];
        return GEOMETRY_FACTORY.createPolygon(coordinates);
    }

    private static Envelope readEnvelope(BasicSliceInput input)
    {
        verify(input.available() > 0);
        double xMin = input.readDouble();
        double yMin = input.readDouble();
        double xMax = input.readDouble();
        double yMax = input.readDouble();

        if (isNaN(xMin) || isNaN(yMin) || isNaN(xMax) || isNaN(yMax)) {
            return new Envelope();
        }

        return new Envelope(xMin, xMax, yMin, yMax);
    }

    private static Coordinate readCoordinate(BasicSliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
    }
}
