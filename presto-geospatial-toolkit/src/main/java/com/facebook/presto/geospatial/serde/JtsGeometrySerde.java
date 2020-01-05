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
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.skipEnvelope;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.writeType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class JtsGeometrySerde
{
    // TODO: Are we sure this is thread safe?
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private JtsGeometrySerde() {}

    public static Geometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
        if (type != GeometrySerializationType.POINT && type != GeometrySerializationType.ENVELOPE) {
            skipEnvelope(input);
        }
        try {
            return readGeometry(input, shape, type, input.available());
        }
        catch (TopologyException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
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
                throw new IllegalArgumentException("Unsupported geometry type: " + type);
        }
    }

    private static Point readPoint(SliceInput input)
    {
        Coordinate coordinate = readCoordinate(input);
        if (isEsriNaN(coordinate.x) || isEsriNaN(coordinate.y)) {
            return GEOMETRY_FACTORY.createPoint();
        }
        return GEOMETRY_FACTORY.createPoint(coordinate);
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

    private static Envelope readEnvelope(SliceInput input)
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

    private static Coordinate readCoordinate(SliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
    }

    private static Coordinate[] readCoordinates(SliceInput input, int count)
    {
        Coordinate[] coordinates = new Coordinate[count];
        for (int i = 0; i < count; i++) {
            coordinates[i] = readCoordinate(input);
        }
        return coordinates;
    }

    /**
     * Serialize JTS {@link Geometry} shape into an ESRI shape
     */
    public static Slice serialize(Geometry geometry)
    {
        requireNonNull(geometry, "input is null");
        DynamicSliceOutput output = new DynamicSliceOutput(100);
        GeometrySerializationType type = getSerializationType(geometry);
        writeType(output, type);
        if (type != GeometrySerializationType.POINT) {
            writeEnvelopeCoordinates(output, geometry.getEnvelopeInternal());
        }
        writeGeometry(output, geometry);
        return output.slice();
    }

    private static GeometrySerializationType getSerializationType(Geometry geometry)
    {
        return GeometrySerializationType.getForGeometryType(GeometryType.getForJtsGeometryType(geometry.getGeometryType()));
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

    private static void writeGeometry(DynamicSliceOutput output, Geometry geometry)
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

    private static void writeGeometryCollection(DynamicSliceOutput output, GeometryCollection collection)
    {
        int numGeometries = collection.getNumGeometries();
        output.appendInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            Geometry geometry = collection.getGeometryN(geometryIndex);
            int startPosition = output.size();
            // leave 4 bytes for the shape length
            output.appendInt(0);

            // Write the Geometry
            writeType(output, getSerializationType(geometry));
            writeGeometry(output, geometry);

            // Retroactively write the geometry length
            int endPosition = output.size();
            int length = endPosition - startPosition - Integer.BYTES;
            output.getUnderlyingSlice().setInt(startPosition, length);
        }
    }

    private static void writeCoordinate(Coordinate coordinate, SliceOutput output)
    {
        output.writeDouble(coordinate.x);
        output.writeDouble(coordinate.y);
    }

    private static void writeCoordinates(Coordinate[] coordinates, SliceOutput output)
    {
        for (Coordinate coordinate : coordinates) {
            writeCoordinate(coordinate, output);
        }
    }

    private static void writeEnvelope(Geometry geometry, SliceOutput output)
    {
        if (geometry.isEmpty()) {
            for (int i = 0; i < 4; i++) {
                output.writeDouble(NaN);
            }
            return;
        }

        Envelope envelope = geometry.getEnvelopeInternal();
        output.writeDouble(envelope.getMinX());
        output.writeDouble(envelope.getMinY());
        output.writeDouble(envelope.getMaxX());
        output.writeDouble(envelope.getMaxY());
    }

    private static void canonicalizePolygonCoordinates(Coordinate[] coordinates, int[] partIndexes, boolean[] shellPart)
    {
        for (int part = 0; part < partIndexes.length - 1; part++) {
            canonicalizePolygonCoordinates(coordinates, partIndexes[part], partIndexes[part + 1], shellPart[part]);
        }
        if (partIndexes.length > 0) {
            canonicalizePolygonCoordinates(coordinates, partIndexes[partIndexes.length - 1], coordinates.length, shellPart[partIndexes.length - 1]);
        }
    }

    private static void canonicalizePolygonCoordinates(Coordinate[] coordinates, int start, int end, boolean isShell)
    {
        boolean isClockwise = isClockwise(coordinates, start, end);

        if ((isShell && !isClockwise) || (!isShell && isClockwise)) {
            // shell has to be counter clockwise
            reverse(coordinates, start, end);
        }
    }

    private static boolean isClockwise(Coordinate[] coordinates)
    {
        return isClockwise(coordinates, 0, coordinates.length);
    }

    private static boolean isClockwise(Coordinate[] coordinates, int start, int end)
    {
        // Sum over the edges: (x2 − x1) * (y2 + y1).
        // If the result is positive the curve is clockwise,
        // if it's negative the curve is counter-clockwise.
        double area = 0;
        for (int i = start + 1; i < end; i++) {
            area += (coordinates[i].x - coordinates[i - 1].x) * (coordinates[i].y + coordinates[i - 1].y);
        }
        area += (coordinates[start].x - coordinates[end - 1].x) * (coordinates[start].y + coordinates[end - 1].y);
        return area > 0;
    }

    private static void reverse(Coordinate[] coordinates, int start, int end)
    {
        verify(start <= end, "start must be less or equal than end");
        for (int i = start; i < start + ((end - start) / 2); i++) {
            Coordinate buffer = coordinates[i];
            coordinates[i] = coordinates[start + end - i - 1];
            coordinates[start + end - i - 1] = buffer;
        }
    }

    /**
     * Shape type codes from ERSI's specification
     * https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
     */
    private enum EsriShapeType
    {
        POINT(1),
        POLYLINE(3),
        POLYGON(5),
        MULTI_POINT(8);

        final int code;

        EsriShapeType(int code)
        {
            this.code = code;
        }
    }
}
