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
package com.facebook.presto.geospatial;

import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.OperatorSimplifyOGC;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

// Tests to explore the behavior of our geometry libraries
public class GeometryTests

{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    public void testMultiPointSimplicity()
    {
        String wkt = "MULTIPOINT (0 0, 1 1, 0 0)";

        Geometry duplicatePointsJts = jtsGeometryFromWkt(wkt);
        assertTrue(duplicatePointsJts.isValid());
        assertFalse(duplicatePointsJts.isSimple());

        OGCGeometry duplicatePointsOgc = ogcGeometryFromWkt(wkt);
        assertFalse(duplicatePointsOgc.isSimple());
        assertFalse(isSimpleEsriOgc(duplicatePointsOgc));
    }

    @Test
    public void testLineStringSimplicity()
    {
        String singlePointWkt = "LINESTRING (0 0)";

        try {
            jtsGeometryFromWkt(singlePointWkt);
        }
        catch (IllegalArgumentException e) {
            // pass
        }

        OGCGeometry singlePointOgc = ogcGeometryFromWkt(singlePointWkt);
        assertFalse(singlePointOgc.isSimple());
        assertFalse(isSimpleEsriOgc(singlePointOgc));

        String duplicatePointsWkt = "LINESTRING (0 0, 0 0)";

        Geometry duplicatePointsJts = jtsGeometryFromWkt(duplicatePointsWkt);
        assertFalse(duplicatePointsJts.isValid());
        assertTrue(duplicatePointsJts.isSimple());

        OGCGeometry duplicatePointsOgc = ogcGeometryFromWkt(duplicatePointsWkt);
        assertFalse(duplicatePointsOgc.isSimple());
        assertFalse(isSimpleEsriOgc(duplicatePointsOgc));

        String duplicatePoints2Wkt = "LINESTRING (0 0, 1 1, 1 1)";

        Geometry duplicatePoints2Jts = jtsGeometryFromWkt(duplicatePoints2Wkt);
        assertTrue(duplicatePoints2Jts.isValid());
        assertTrue(duplicatePoints2Jts.isSimple());

        OGCGeometry duplicatePoints2Ogc = ogcGeometryFromWkt(duplicatePoints2Wkt);
        assertFalse(duplicatePoints2Ogc.isSimple());
        assertFalse(isSimpleEsriOgc(duplicatePoints2Ogc));

        String selfIntersectionWkt = "LINESTRING (0 0, 1 1, 1 0, 0 1)";
        Geometry selfIntersectionJts = jtsGeometryFromWkt(selfIntersectionWkt);
        assertTrue(selfIntersectionJts.isValid());
        assertFalse(selfIntersectionJts.isSimple());

        OGCGeometry selfIntersectionOgc = ogcGeometryFromWkt(selfIntersectionWkt);
        assertFalse(selfIntersectionOgc.isSimple());
        assertFalse(isSimpleEsriOgc(selfIntersectionOgc));
    }

    @Test
    public void testBufferEdgeCases()
    {
        // Empty Geometries
        // ISO Spec states buffer on empty geometries returns null
        assertTrue(getJtsBufferedGeometry("POINT EMPTY", 1).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT EMPTY", 1).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT EMPTY", -1).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT EMPTY", -1).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT EMPTY", Double.NaN).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT EMPTY", Double.NaN).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT EMPTY", Double.POSITIVE_INFINITY).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT EMPTY", Double.POSITIVE_INFINITY).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT EMPTY", Double.NEGATIVE_INFINITY).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT EMPTY", Double.NEGATIVE_INFINITY).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT EMPTY", Double.MIN_VALUE).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT EMPTY", Double.MIN_VALUE).isEmpty());

        // Negative distances
        assertTrue(getJtsBufferedGeometry("POINT (0 0)", -1).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT (0 0)", -1).isEmpty());

        // Non-normal distances
        assertTrue(getJtsBufferedGeometry("POINT (0 0)", Double.NaN).isEmpty());
        try {
            getOgcBufferedGeometry("POINT (0 0)", Double.NaN);
        }
        catch (AssertionError e) {
            // Pass
        }

        assertTrue(getJtsBufferedGeometry("POINT (0 0)", Double.POSITIVE_INFINITY).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT (0 0)", Double.POSITIVE_INFINITY).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT (0 0)", Double.NEGATIVE_INFINITY).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT (0 0)", Double.NEGATIVE_INFINITY).isEmpty());

        assertTrue(getJtsBufferedGeometry("POINT (0 0)", Double.MIN_VALUE).isEmpty());
        assertTrue(getOgcBufferedGeometry("POINT (0 0)", Double.MIN_VALUE).isEmpty());
    }

    @Test
    public void testEnvelope()
    {
        // Empty Envelopes
        // ISO Spec: Empty geometries return null envelopes
        assertTrue(jtsGeometryFromWkt("POINT EMPTY").getEnvelopeInternal().isNull());
        assertTrue(jtsGeometryFromWkt("POINT EMPTY").getEnvelope().isEmpty());
        assertTrue(ogcGeometryFromWkt("POINT EMPTY").envelope().isEmpty());
    }

    @Test
    public void testNumPoints()
    {
        assertEquals(jtsGeometryFromWkt("LINESTRING (0 0, 1 1, 1 0, 0 0)").getNumPoints(), 4);
        MultiVertexGeometry esriLineString = (MultiVertexGeometry) ogcGeometryFromWkt("LINESTRING (0 0, 1 1, 1 0, 0 0)").getEsriGeometry();
        assertEquals(esriLineString.getPointCount(), 4);
    }

    private Geometry jtsGeometryFromWkt(String wkt)
    {
        try {
            return new WKTReader(GEOMETRY_FACTORY).read(wkt);
        }
        catch (ParseException e) {
            throw new RuntimeException("Invalid WKT: " + wkt, e);
        }
    }

    private OGCGeometry ogcGeometryFromWkt(String wkt)
    {
        return OGCGeometry.fromText(wkt);
    }

    private Geometry getJtsBufferedGeometry(String wkt, double distance)
    {
        return jtsGeometryFromWkt(wkt).buffer(distance);
    }

    private OGCGeometry getOgcBufferedGeometry(String wkt, double distance)
    {
        return ogcGeometryFromWkt(wkt).buffer(distance);
    }

    private boolean isSimpleEsriOgc(OGCGeometry geometry)
    {
        return OperatorSimplifyOGC.local().isSimpleOGC(
                geometry.getEsriGeometry(),
                null,
                true,
                null,
                null);
    }
}
