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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.VertexDescription;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.geospatial.GeometryUtils;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.readType;
import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.skipEnvelope;
import static com.facebook.presto.geospatial.serde.GeometrySerdeUtils.writeType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * We serialize geometries in a WKB-like fashion.  We don't use strict WKB
 * because it cannot support POINT EMPTY geometries.  These may occur in two
 * places: As a Point object, or as an entry in a GeometryCollection.  The
 * latter is not a valid geometry, but we need to handle the logical possibility.
 * Each serialization is prefixed by a 1-byte GeometrySerializationType which
 * controls how we serialize.
 *
 * Our serialization grammar is as follows:
 * TYPE: 1 byte: GeometrySerializationType.code
 * COORDINATE: 8 bytes: double (x or y, not both)
 * BOUNDS: 32 bytes: COORDINATE*4
 * NUMBER: 4 bytes: int number of geometries in a collection
 * COORD_SEQ: variable (4 + 16 * N): NUMBER, COORDINATE * 2 * NUMBER
 *
 * POINT: 17 bytes: TYPE, COORDINATE*2
 * ENVELOPE: 33 bytes: TYPE, BOUNDS
 * MULTIPOINT: variable (37 + 16 * N): TYPE, BOUNDS, COORD_SEQ
 * LINESTRING: variable: TYPE, BOUNDS, COORD_SEQ
 * MULTILINESTRING: variable: TYPE, BOUNDS, NUM_LINESTRINGS, COORD_SEQ*NUM_LINESTRINGS
 * POLYGON: variable: TYPE, BOUNDS, POLYGON_INNER
 * POLYGON_INNER: variable: COORD_SEQ, NUM_INTERIORS, COORD_SEQ*NUM_INTERIORS
 * MULTIPOLYGON: variable: TYPE, BOUNDS, NUMBER, POLYGON_INNER*NUMBER
 * COLLECTION: variable: TYPE,BOUNDS,NUMBER,COLLECTION_INNER*NUMBER
 * COLLECTION_INNER: variable: LENGTH,GEOMETRY_INNER
 * GEOMETRY_INNER: variable: Geometry without BOUNDS
 *
 * SERIALIZED_GEOMETRY: variable: one of the geometries above
 */
public class GeometrySerde
{
    private GeometrySerde() {}

    public static Slice serialize(OGCGeometry geometry)
    {
        requireNonNull(geometry, "geometry is null");
        DynamicSliceOutput output = new DynamicSliceOutput(100);
        GeometrySerializationType type = GeometrySerializationType.of(geometry);
        writeType(output, type);
        if (type != GeometrySerializationType.POINT) {
            writeEnvelopeCoordinates(output, GeometryUtils.getEnvelope(geometry));
        }
        writeGeometry(output, geometry);
        return output.slice();
    }

    public static Slice serialize(Envelope envelope)
    {
        requireNonNull(envelope, "envelope is null");
        verify(!envelope.isEmpty());
        DynamicSliceOutput output = new DynamicSliceOutput(33);
        writeType(output, GeometrySerializationType.ENVELOPE);
        writeEnvelopeCoordinates(output, envelope);
        return output.slice();
    }

    private static void writeEnvelopeCoordinates(SliceOutput output, Envelope envelope)
    {
        if (envelope.isEmpty()) {
            output.writeDouble(NaN);
            output.writeDouble(NaN);
            output.writeDouble(NaN);
            output.writeDouble(NaN);
        }
        else {
            output.writeDouble(envelope.getXMin());
            output.writeDouble(envelope.getYMin());
            output.writeDouble(envelope.getXMax());
            output.writeDouble(envelope.getYMax());
        }
    }

    private static void writeGeometry(SliceOutput output, OGCGeometry geometry)
    {
        switch (GeometryType.getForEsriGeometryType(geometry.geometryType())) {
            case POINT:
                writePoint(output, geometry);
                break;
            case MULTI_POINT:
                writeMultiVertex(output, (MultiPoint) geometry.getEsriGeometry());
                break;
            case LINE_STRING:
                writeMultiVertex(output, (MultiPath) geometry.getEsriGeometry());
                break;
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
                writeSimpleGeometry(output, geometry);
                break;
            case GEOMETRY_COLLECTION:
                verify(geometry instanceof OGCConcreteGeometryCollection);
                writeGeometryCollection(output, (OGCConcreteGeometryCollection) geometry);
                break;
            default:
                throw new IllegalArgumentException("Unsupported geometry type : " + geometry.geometryType());
        }
    }

    private static void writePoint(SliceOutput output, OGCGeometry geometry)
    {
        Point point = (Point) geometry.getEsriGeometry();
        if (!point.isEmpty()) {
            output.appendDouble(point.getX());
            output.appendDouble(point.getY());
        }
        else {
            output.appendDouble(NaN);
            output.appendDouble(NaN);
        }
    }

    private static void writeMultiVertex(SliceOutput output, MultiVertexGeometry geometry)
    {
        int numPoints = geometry.getPointCount();
        Slices.ensureSize(output.getUnderlyingSlice(), SIZE_OF_INT + SIZE_OF_DOUBLE * 2 * numPoints);
        output.writeInt(numPoints);
        for (int i = 0; i < numPoints; i++) {
            Point2D point = new Point2D();
            geometry.getXY(i, point);
            output.writeDouble(point.x);
            output.writeDouble(point.y);
        }
    }

    private static void writeMultiLineString(SliceOutput output, OGCGeometry geometry)
    {
        Polyline polyline = (Polyline) geometry.getEsriGeometry();
    }

    private static void writeSimpleGeometry(SliceOutput output, OGCGeometry geometry)
    {
        output.appendBytes(geometry.asBinary().array());
    }

    private static void writeGeometryCollection(SliceOutput output, OGCConcreteGeometryCollection collection)
    {
        int numGeometries = collection.numGeometries();
        output.appendInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            OGCGeometry geometry = collection.geometryN(geometryIndex);
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

    public static GeometrySerializationType deserializeType(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        return GeometrySerializationType.getForCode(input.readByte());
    }

    public static GeometryType getGeometryType(Slice shape)
    {
        return deserializeType(shape).geometryType();
    }

    public static OGCGeometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        GeometrySerializationType type = readType(input);
        if (type != GeometrySerializationType.POINT && type != GeometrySerializationType.ENVELOPE) {
            skipEnvelope(input);
        }
        return readGeometry(input, shape, type, input.available());
    }

    @Nullable
    public static Envelope deserializeEnvelope(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);

        GeometrySerializationType type = readType(input);
        switch (type) {
            case POINT:
                return readPointEnvelope(input);
            case MULTI_POINT:
            case LINE_STRING:
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
            case GEOMETRY_COLLECTION:
                return readSimpleGeometryEnvelope(input);
            case ENVELOPE:
                return readEnvelope(input);
            default:
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected type in deserialization: " + type);
        }
    }

    private static OGCGeometry readGeometry(BasicSliceInput input, Slice shape, GeometrySerializationType type, int length)
    {
        switch (type) {
            case POINT:
                return readPoint(input);
            case MULTI_POINT:
                return readMultiPoint(input);
            case LINE_STRING:
                return readLineString(input);
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

    private static OGCPoint readPoint(BasicSliceInput input)
    {
        double x = input.readDouble();
        double y = input.readDouble();
        Point point;
        if (isNaN(x) || isNaN(y)) {
            point = new Point();
        }
        else {
            point = new Point(x, y);
        }
        return new OGCPoint(point, null);
    }

    private static OGCMultiPoint readMultiPoint(BasicSliceInput input)
    {
        int numPoints = input.readInt();
        MultiPoint multipoint = new MultiPoint();
        for (int i = 0; i < numPoints; i++) {
             multipoint.add(input.readDouble(), input.readDouble());
        }
        return new OGCMultiPoint(multipoint, null);
    }

    private static OGCLineString readLineString(BasicSliceInput input)
    {
        int numPoints = input.readInt();
        Polyline polyline = new Polyline();
        if (numPoints > 0) {
            polyline.startPath(input.readDouble(), input.readDouble());
            for (int i = 1; i < numPoints; i++) {
                polyline.lineTo(input.readDouble(), input.readDouble());
            }
        }
        return new OGCLineString(polyline, 0, null);
    }

    private static OGCGeometry readSimpleGeometry(BasicSliceInput input, Slice shape, int length)
    {
        ByteBuffer geometryBuffer = shape.toByteBuffer(toIntExact(input.position()), length).slice();
        input.skip(length);
        return OGCGeometry.fromBinary(geometryBuffer);
    }

    private static OGCGeometryCollection readGeometryCollection(BasicSliceInput input, Slice inputSlice)
    {
        int numGeometries = input.readInt();
        List<OGCGeometry> geometries = new ArrayList<>(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            int length = input.readInt();
            GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
            geometries.add(readGeometry(input, inputSlice, type, length - 1));
        }
        return new OGCConcreteGeometryCollection(geometries, null);
    }

    private static OGCGeometry createPolygonFromEnvelope(Envelope envelope)
    {
        Polygon polygon = new Polygon();
        polygon.addEnvelope(envelope, false);
        return new OGCPolygon(polygon, null);
    }

    private static Envelope readSimpleGeometryEnvelope(BasicSliceInput input)
    {
        Envelope envelope = readEnvelope(input);
        input.setPosition(input.length() - 1);
        return envelope;
    }

    private static Envelope readPointEnvelope(BasicSliceInput input)
    {
        double x = input.readDouble();
        double y = input.readDouble();
        if (isNaN(x) || isNaN(y)) {
            return new Envelope();
        }
        return new Envelope(x, y, x, y);
    }

    private static Envelope readEnvelope(BasicSliceInput input)
    {
        verify(input.available() > 0);
        double xMin = input.readDouble();
        double yMin = input.readDouble();
        double xMax = input.readDouble();
        double yMax = input.readDouble();
        if (isEsriNaN(xMin) || isEsriNaN(yMin) || isEsriNaN(xMin) || isEsriNaN(yMin)) {
            return new Envelope();
        }
        return new Envelope(xMin, yMin, xMax, yMax);
    }
}
