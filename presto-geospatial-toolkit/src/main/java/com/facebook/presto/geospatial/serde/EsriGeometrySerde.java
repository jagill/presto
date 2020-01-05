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
import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.VertexDescription;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.geospatial.GeometryUtils;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static java.lang.Double.NaN;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class EsriGeometrySerde
{
    private EsriGeometrySerde() {}

    public static Slice serialize(OGCGeometry input)
    {
        requireNonNull(input, "input is null");
        DynamicSliceOutput output = new DynamicSliceOutput(100);
        GeometrySerializationType type = getSerializationType(input);
        writeType(output, type);
        if (type != GeometrySerializationType.POINT) {
            writeEnvelopeCoordinates(output, GeometryUtils.getEnvelope(input));
        }
        writeGeometry(output, input);
        return output.slice();
    }

    public static Slice serialize(Envelope envelope)
    {
        requireNonNull(envelope, "envelope is null");
        DynamicSliceOutput output = new DynamicSliceOutput(33);
        writeType(output, GeometrySerializationType.ENVELOPE);
        writeEnvelopeCoordinates(output, envelope);
        return output.slice();
    }

    public static GeometryType getGeometryType(Slice shape)
    {
        return deserializeType(shape).geometryType();
    }

    private static GeometrySerializationType getSerializationType(OGCGeometry geometry)
    {
        return GeometrySerializationType.getForGeometryType(GeometryType.getForEsriGeometryType(geometry.geometryType()));
    }

    static void writeType(DynamicSliceOutput output, GeometrySerializationType type)
    {
        output.writeByte(type.code());
    }

    private static void writeGeometry(DynamicSliceOutput output, OGCGeometry geometry)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        switch (type) {
            case POINT:
                writePoint(output, geometry);
                break;
            case MULTI_POINT:
            case LINE_STRING:
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
                throw new IllegalArgumentException("Unsupported geometry type: " + type);
        }
    }

    private static void writeGeometryCollection(DynamicSliceOutput output, OGCGeometryCollection collection)
    {
        int numGeometries = collection.numGeometries();
        output.appendInt(numGeometries);
        for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++) {
            OGCGeometry geometry = collection.geometryN(geometryIndex);
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

    private static void writeSimpleGeometry(DynamicSliceOutput output, OGCGeometry geometry)
    {
        output.appendBytes(geometry.asBinary().array());
    }

    private static void writePoint(DynamicSliceOutput output, OGCGeometry geometry)
    {
        Geometry esriGeometry = geometry.getEsriGeometry();
        verify(esriGeometry instanceof Point, "geometry is expected to be an instance of Point");
        Point point = (Point) esriGeometry;
        verify(!point.hasAttribute(VertexDescription.Semantics.Z) &&
                        !point.hasAttribute(VertexDescription.Semantics.M) &&
                        !point.hasAttribute(VertexDescription.Semantics.ID),
                "Only 2D points with no ID nor M attribute are supported");
        if (!point.isEmpty()) {
            output.appendDouble(point.getX());
            output.appendDouble(point.getY());
        }
        else {
            output.appendDouble(NaN);
            output.appendDouble(NaN);
        }
    }

    public static GeometrySerializationType deserializeType(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        return GeometrySerializationType.getForCode(input.readByte());
    }

    public static OGCGeometry deserialize(Slice shape)
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
        catch (GeometryException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    private static OGCGeometry readGeometry(BasicSliceInput input, Slice shape, GeometrySerializationType type, int length)
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

    private static OGCConcreteGeometryCollection readGeometryCollection(BasicSliceInput input, Slice inputSlice)
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

    private static OGCGeometry readSimpleGeometry(BasicSliceInput input, Slice shape, int length)
    {
        ByteBuffer geometryBuffer = shape.toByteBuffer(
                toIntExact(input.position()), length).slice();
        input.skip(length);
        return OGCGeometry.fromBinary(geometryBuffer);
    }

    private static OGCGeometry createPolygonFromEnvelope(Envelope envelope)
    {
        Polygon polygon = new Polygon();
        polygon.addEnvelope(envelope, false);
        return new OGCPolygon(polygon, null);
    }

    @VisibleForTesting
    static OGCGeometry createFromEsriGeometry(Geometry geometry, boolean multiType)
    {
        Geometry.Type type = geometry.getType();
        switch (type) {
            case Polygon: {
                if (!multiType && ((Polygon) geometry).getExteriorRingCount() <= 1) {
                    return new OGCPolygon((Polygon) geometry, null);
                }
                return new OGCMultiPolygon((Polygon) geometry, null);
            }
            case Polyline: {
                if (!multiType && ((Polyline) geometry).getPathCount() <= 1) {
                    return new OGCLineString((Polyline) geometry, 0, null);
                }
                return new OGCMultiLineString((Polyline) geometry, null);
            }
            case MultiPoint: {
                if (!multiType && ((MultiPoint) geometry).getPointCount() <= 1) {
                    if (geometry.isEmpty()) {
                        return new OGCPoint(new Point(), null);
                    }
                    return new OGCPoint(((MultiPoint) geometry).getPoint(0), null);
                }
                return new OGCMultiPoint((MultiPoint) geometry, null);
            }
            case Point: {
                if (!multiType) {
                    return new OGCPoint((Point) geometry, null);
                }
                return new OGCMultiPoint((Point) geometry, null);
            }
            case Envelope: {
                Polygon polygon = new Polygon();
                polygon.addEnvelope((Envelope) geometry, false);
                return new OGCPolygon(polygon, null);
            }
            default:
                throw new IllegalArgumentException("Unexpected geometry type: " + type);
        }
    }

    private static OGCPoint readPoint(BasicSliceInput input)
    {
        double x = input.readDouble();
        double y = input.readDouble();
        Point point;
        if (isEsriNaN(x) || isEsriNaN(y)) {
            point = new Point();
        }
        else {
            point = new Point(x, y);
        }
        return new OGCPoint(point, null);
    }

    @Nullable
    public static Envelope deserializeEnvelope(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);

        GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
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
                throw new IllegalArgumentException("Unexpected geometry type: " + type);
        }
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
        if (isEsriNaN(x) || isEsriNaN(y)) {
            return new Envelope();
        }
        return new Envelope(x, y, x, y);
    }

    private static Envelope readEnvelope(SliceInput input)
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

    private static void writeEnvelopeCoordinates(DynamicSliceOutput output, Envelope envelope)
    {
        if (envelope.isEmpty()) {
            output.appendDouble(NaN);
            output.appendDouble(NaN);
            output.appendDouble(NaN);
            output.appendDouble(NaN);
        }
        else {
            output.appendDouble(envelope.getXMin());
            output.appendDouble(envelope.getYMin());
            output.appendDouble(envelope.getXMax());
            output.appendDouble(envelope.getYMax());
        }
    }

    static void skipEnvelope(BasicSliceInput input)
    {
        requireNonNull(input, "input is null");
        int skipLength = 4 * SIZE_OF_DOUBLE;
        verify(input.skip(skipLength) == skipLength);
    }
}
