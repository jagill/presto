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
import io.airlift.slice.Slices;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.readCoordinates;
import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.readType;
import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.skipEnvelope;
import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.writeType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * We serialize JTS geometries similar to ESRI geometries.
 * @see com.facebook.presto.geospatial.serde.GeometrySerde
 */
public class JtsGeometrySerde
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PackedCoordinateSequenceFactory());

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
                writeMultiPoint(output, (MultiPoint) geometry);
                break;
            case LINE_STRING:
                writeLineString(output, (LineString) geometry);
                break;
            case MULTI_LINE_STRING:
                writeMultiLineString(output, (MultiLineString) geometry);
                break;
            case POLYGON:
                writePolygon(output, (Polygon) geometry);
                break;
            case MULTI_POLYGON:
                writeMultiPolygon(output, (MultiPolygon) geometry);
                break;
            case GEOMETRY_COLLECTION:
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

    private static void writeMultiPoint(SliceOutput output, MultiPoint multipoint)
    {
        int numGeometries = multipoint.getNumGeometries();
        Slices.ensureSize(output.getUnderlyingSlice(), SIZE_OF_INT + numGeometries * 2 * SIZE_OF_DOUBLE);
        output.writeInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            writePoint(output, (Point) multipoint.getGeometryN(geometryIndex));
        }
    }

    private static void writeLineString(SliceOutput output, LineString linestring)
    {
        writeCoordinateSequence(output, linestring.getCoordinateSequence());
    }

    private static void writeCoordinateSequence(SliceOutput output, CoordinateSequence coordinateSequence)
    {
        int numCoordinates = coordinateSequence.size();
        Slices.ensureSize(output.getUnderlyingSlice(), SIZE_OF_INT + numCoordinates * 2 * SIZE_OF_DOUBLE);
        output.writeInt(numCoordinates);
        for (int i = 0; i < numCoordinates; i++) {
            output.writeDouble(coordinateSequence.getX(i));
            output.writeDouble(coordinateSequence.getY(i));
        }
    }

    private static void writeMultiLineString(SliceOutput output, MultiLineString multilinestring)
    {
        int numGeometries = multilinestring.getNumGeometries();
        output.writeInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            writeLineString(output, (LineString) multilinestring.getGeometryN(geometryIndex));
        }
    }

    public static void writePolygon(SliceOutput output, Polygon polygon)
    {
        writeLineString(output, polygon.getExteriorRing());
        int numInteriorRings = polygon.getNumInteriorRing();
        output.writeInt(numInteriorRings);
        for (int geometryIndex = 0; geometryIndex < numInteriorRings; geometryIndex++) {
            writeLineString(output, polygon.getInteriorRingN(geometryIndex));
        }
    }

    public static void writeMultiPolygon(SliceOutput output, MultiPolygon multipolygon)
    {
        int numGeometries = multipolygon.getNumGeometries();
        output.writeInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            writePolygon(output, (Polygon) multipolygon.getGeometryN(geometryIndex));
        }
    }

    private static void writeGeometryCollection(SliceOutput output, GeometryCollection collection)
    {
        int numGeometries = collection.getNumGeometries();
        output.writeInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            Geometry geometry = collection.getGeometryN(geometryIndex);
            GeometrySerializationType type = GeometrySerializationType.of(geometry);
            writeType(output, type);
            writeGeometry(output, geometry);
        }
    }

    public static Geometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        GeometrySerializationType type = readType(input);
        if (type != GeometrySerializationType.POINT && type != GeometrySerializationType.ENVELOPE) {
            skipEnvelope(input);
        }
        return readGeometry(input, type);
    }

    public static Envelope deserializeEnvelope(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        GeometrySerializationType type = readType(input);
        if (type == GeometrySerializationType.POINT) {
            return readPointEnvelope(input);
        } else {
            return readEnvelope(input);
        }
    }

    private static Geometry readGeometry(BasicSliceInput input, GeometrySerializationType type)
    {
        switch (type) {
            case POINT:
                return readPoint(input);
            case MULTI_POINT:
                return readMultiPoint(input);
            case LINE_STRING:
                return readLineString(input);
            case MULTI_LINE_STRING:
                return readMultiLineString(input);
            case POLYGON:
                return readPolygon(input);
            case MULTI_POLYGON:
                return readMultiPolygon(input);
            case GEOMETRY_COLLECTION:
                return readGeometryCollection(input);
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

    private static MultiPoint readMultiPoint(BasicSliceInput input)
    {
        int numPoints = input.readInt();
        Point[] points = new Point[numPoints];
        for (int i = 0; i < numPoints; i++) {
            points[i] = readPoint(input);
        }
        return GEOMETRY_FACTORY.createMultiPoint(points);
    }

    private static LineString readLineString(BasicSliceInput input)
    {
        return GEOMETRY_FACTORY.createLineString(readCoordinateSequence(input));
    }

    private static MultiLineString readMultiLineString(BasicSliceInput input)
    {
        int numLineStrings = input.readInt();
        LineString[] lineStrings = new LineString[numLineStrings];
        for (int i = 0; i < numLineStrings; i++) {
            lineStrings[i] = readLineString(input);
        }
        return GEOMETRY_FACTORY.createMultiLineString(lineStrings);
    }

    private static Polygon readPolygon(BasicSliceInput input)
    {
        LinearRing exterior = GEOMETRY_FACTORY.createLinearRing(readCoordinateSequence(input));
        int numHoles = input.readInt();
        LinearRing[] holes = new LinearRing[numHoles];
        for (int i = 0; i < numHoles; i++) {
            holes[i] = GEOMETRY_FACTORY.createLinearRing(readCoordinateSequence(input));
        }
        return GEOMETRY_FACTORY.createPolygon(exterior, holes);
    }

    private static MultiPolygon readMultiPolygon(BasicSliceInput input)
    {
        int numPolygons = input.readInt();
        Polygon[] polygons = new Polygon[numPolygons];
        for (int i = 0; i < numPolygons; i++) {
            polygons[i] = readPolygon(input);
        }

        return GEOMETRY_FACTORY.createMultiPolygon(polygons);
    }

    private static GeometryCollection readGeometryCollection(BasicSliceInput input)
    {
        int numGeometries = input.readInt();
        Geometry[] geometries = new Geometry[numGeometries];
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
            geometries[geometryIndex] = readGeometry(input, type);
        }
        return GEOMETRY_FACTORY.createGeometryCollection(geometries);
    }

    private static CoordinateSequence readCoordinateSequence(BasicSliceInput input)
    {
        double[] coordinates = readCoordinates(input);
        return new PackedCoordinateSequence.Double(coordinates, 2, 0);
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
        return new Coordinate(input.readDouble(), input.readDouble());
    }

    private static Envelope readPointEnvelope(SliceInput input)
    {
        double x = input.readDouble();
        double y = input.readDouble();
        if (isNaN(x) || isNaN(y)) {
            return new Envelope();
        }
        return new Envelope(x, y, x, y);
    }
}
