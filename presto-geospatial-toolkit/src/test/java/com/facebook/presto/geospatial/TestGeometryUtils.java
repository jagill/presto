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

import com.esri.core.geometry.GeometryCursorAppend;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorUnion;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.locationtech.jts.geom.Envelope;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.flattenCollection;
import static com.facebook.presto.geospatial.GeometryUtils.getExtent;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestGeometryUtils
{
    @Test
    public void testGetJtsEnvelope()
    {
        assertJtsEnvelope(
                "MULTIPOLYGON EMPTY",
                new Envelope());
        assertJtsEnvelope(
                "POINT (-23.4 12.2)",
                new Envelope(-23.4, -23.4, 12.2, 12.2));
        assertJtsEnvelope(
                "LINESTRING (-75.9375 23.6359, -75.9375 23.6364)",
                new Envelope(-75.9375, -75.9375, 23.6359, 23.6364));
        assertJtsEnvelope(
                "GEOMETRYCOLLECTION (" +
                        "  LINESTRING (-75.9375 23.6359, -75.9375 23.6364)," +
                        "  MULTIPOLYGON (((-75.9375 23.45520, -75.9371 23.4554, -75.9375 23.46023325, -75.9375 23.45520)))" +
                        ")",
                new Envelope(-75.9375, -75.9371, 23.4552, 23.6364));
    }

    private void assertJtsEnvelope(String wkt, Envelope expected)
    {
        Envelope calculated = GeometryUtils.getJtsEnvelope(OGCGeometry.fromText(wkt));
        assertEquals(calculated, expected);
    }

    @Test
    public void testGetExtent()
    {
        assertGetExtent(
                "POINT (-23.4 12.2)",
                new Rectangle(-23.4, 12.2, -23.4, 12.2));
        assertGetExtent(
                "LINESTRING (-75.9375 23.6359, -75.9375 23.6364)",
                new Rectangle(-75.9375, 23.6359, -75.9375, 23.6364));
        assertGetExtent(
                "GEOMETRYCOLLECTION (" +
                        "  LINESTRING (-75.9375 23.6359, -75.9375 23.6364)," +
                        "  MULTIPOLYGON (((-75.9375 23.45520, -75.9371 23.4554, -75.9375 23.46023325, -75.9375 23.45520)))" +
                        ")",
                new Rectangle(-75.9375, 23.4552, -75.9371, 23.6364));
    }

    private void assertGetExtent(String wkt, Rectangle expected)
    {
        assertEquals(getExtent(OGCGeometry.fromText(wkt)), expected);
    }

    @Test
    public void testFlattenCollection()
    {
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POINT EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POINT (1 2)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOINT EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOINT (1 2)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOINT (1 2, 3 4)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("LINESTRING EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("LINESTRING (1 2, 3 4)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTILINESTRING EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTILINESTRING ((1 2, 3 4))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POLYGON EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POLYGON ((0 0, 0 1, 1 1, 0 0))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOLYGON EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOLYGON (((0 0, 0 1, 1 1, 0 0)))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOLYGON (((0 0, 0 1, 1 1, 0 0)), ((10 10, 10 11, 11 11, 10 10)))"));

        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION EMPTY"), ImmutableList.of());
        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION (POINT EMPTY)"), OGCGeometry.fromText("POINT EMPTY"));
        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION (POINT (0 1))"), OGCGeometry.fromText("POINT (0 1)"));
        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)"), ImmutableList.of());
        assertFlattens(
                OGCGeometry.fromText("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (0 1), POINT (1 2)))"),
                ImmutableList.of(OGCGeometry.fromText("POINT (0 1)"), OGCGeometry.fromText("POINT (1 2)")));
    }

    private void assertFlattenLeavesUnchanged(OGCGeometry original)
    {
        assertFlattens(original, original);
    }

    private void assertFlattens(OGCGeometry original, OGCGeometry expected)
    {
        assertFlattens(original, ImmutableList.of(expected));
    }

    private void assertFlattens(OGCGeometry original, List<OGCGeometry> expected)
    {
        List<String> expectedWkts = expected.stream().map(g -> g.toString()).sorted().collect(toImmutableList());
        List<String> result = Streams.stream(flattenCollection(original)).map(g -> g.toString()).sorted().collect(toImmutableList());
        assertEquals(result, expectedWkts);
    }

    @Test
    public void testHangingUnion()
    {
        OGCGeometry point = OGCGeometry.fromText("POINT (-44.16176186699087 -19.943264803833348)");
//        OGCGeometry lineString = OGCGeometry.fromText("LINESTRING (-44.1247493 -19.9467657, -44.1247979 -19.9468385, -44.1249043 -19.946934, -44.1251096 -19.9470651, -44.1252609 -19.9471383, -44.1254992 -19.947204, -44.1257652 -19.947229, -44.1261292 -19.9471833, -44.1268946 -19.9470098, -44.1276847 -19.9468416, -44.127831 -19.9468143, -44.1282639 -19.9467366, -44.1284569 -19.9467237, -44.1287119 -19.9467261, -44.1289437 -19.9467665, -44.1291499 -19.9468221, -44.1293856 -19.9469396, -44.1298857 -19.9471497, -44.1300908 -19.9472071, -44.1302743 -19.9472331, -44.1305029 -19.9472364, -44.1306498 -19.9472275, -44.1308054 -19.947216, -44.1308553 -19.9472037, -44.1313206 -19.9471394, -44.1317889 -19.9470854, -44.1330422 -19.9468887, -44.1337465 -19.9467083, -44.1339922 -19.9466842, -44.1341506 -19.9466997, -44.1343621 -19.9467226, -44.1345134 -19.9467855, -44.1346494 -19.9468456, -44.1347295 -19.946881, -44.1347988 -19.9469299, -44.1350231 -19.9471131, -44.1355843 -19.9478307, -44.1357802 -19.9480557, -44.1366289 -19.949198, -44.1370384 -19.9497001, -44.137386 -19.9501921, -44.1374113 -19.9502263, -44.1380888 -19.9510925, -44.1381769 -19.9513526, -44.1382509 -19.9516202, -44.1383014 -19.9522136, -44.1383889 -19.9530931, -44.1384227 -19.9538784, -44.1384512 -19.9539653, -44.1384555 -19.9539807, -44.1384901 -19.9541928, -44.1385563 -19.9543859, -44.1386656 -19.9545781, -44.1387339 -19.9546889, -44.1389219 -19.9548661, -44.1391695 -19.9550384, -44.1393672 -19.9551414, -44.1397538 -19.9552208, -44.1401714 -19.9552332, -44.1405656 -19.9551143, -44.1406198 -19.9550853, -44.1407579 -19.9550224, -44.1409029 -19.9549201, -44.1410283 -19.9548257, -44.1413902 -19.9544132, -44.141835 -19.9539274, -44.142268 -19.953484, -44.1427036 -19.9531023, -44.1436229 -19.952259, -44.1437568 -19.9521565, -44.1441783 -19.9517273, -44.144644 -19.9512109, -44.1452538 -19.9505663, -44.1453541 -19.9504774, -44.1458653 -19.9500442, -44.1463563 -19.9496473, -44.1467534 -19.9492812, -44.1470553 -19.9490028, -44.1475804 -19.9485293, -44.1479838 -19.9482096, -44.1485003 -19.9478532, -44.1489451 -19.9477314, -44.1492225 -19.9477024, -44.149453 -19.9476684, -44.149694 -19.9476387, -44.1499556 -19.9475436, -44.1501398 -19.9474234, -44.1502723 -19.9473206, -44.150421 -19.9471473, -44.1505043 -19.9470004, -44.1507664 -19.9462594, -44.150867 -19.9459518, -44.1509225 -19.9457843, -44.1511168 -19.945466, -44.1513601 -19.9452272, -44.1516846 -19.944999, -44.15197 -19.9448738, -44.1525994 -19.9447263, -44.1536614 -19.9444791, -44.1544071 -19.9442671, -44.1548978 -19.9441275, -44.1556247 -19.9438304, -44.1565996 -19.9434083, -44.1570351 -19.9432556, -44.1573142 -19.9432091, -44.1575332 -19.9431645, -44.157931 -19.9431484, -44.1586408 -19.9431504, -44.1593575 -19.9431457, -44.1596498 -19.9431562, -44.1600991 -19.9431475, -44.1602331 -19.9431567, -44.1607926 -19.9432449, -44.1609723 -19.9432499, -44.1623815 -19.9432765, -44.1628299 -19.9433645, -44.1632475 -19.9435839, -44.1633456 -19.9436559, -44.1636261 -19.9439375, -44.1638186 -19.9442439, -44.1642535 -19.9451781, -44.165178 -19.947156, -44.1652928 -19.9474016, -44.1653074 -19.9474329, -44.1654026 -19.947766, -44.1654774 -19.9481718, -44.1655699 -19.9490241, -44.1656196 -19.9491538, -44.1659735 -19.9499097, -44.1662485 -19.9504925, -44.1662996 -19.9506347, -44.1663574 -19.9512961, -44.1664094 -19.9519273, -44.1664144 -19.9519881, -44.1664799 -19.9526399, -44.1666965 -19.9532586, -44.1671191 -19.9544126, -44.1672019 -19.9545869, -44.1673344 -19.9547603, -44.1675958 -19.9550466, -44.1692349 -19.9567775, -44.1694607 -19.9569284, -44.1718843 -19.9574147, -44.1719167 -19.9574206, -44.1721627 -19.9574748, -44.1723207 -19.9575386, -44.1724439 -19.9575883, -44.1742798 -19.9583293, -44.1748841 -19.9585688, -44.1751118 -19.9586796, -44.1752554 -19.9587769, -44.1752644 -19.9587881, -44.1756052 -19.9592143, -44.1766415 -19.9602689, -44.1774912 -19.9612387, -44.177663 -19.961364, -44.177856 -19.9614494, -44.178034 -19.9615125, -44.1782475 -19.9615423, -44.1785115 -19.9615155, -44.1795404 -19.9610879, -44.1796393 -19.9610759, -44.1798873 -19.9610459, -44.1802404 -19.961036, -44.1804714 -19.9609634, -44.181059 -19.9605365, -44.1815113 -19.9602333, -44.1826712 -19.9594067, -44.1829715 -19.9592551, -44.1837201 -19.9590611, -44.1839277 -19.9590073, -44.1853022 -19.9586512, -44.1856812 -19.9585316, -44.1862915 -19.9584212, -44.1866215 -19.9583494, -44.1867651 -19.9583391, -44.1868852 -19.9583372, -44.1872523 -19.9583313, -44.187823 -19.9583281, -44.1884457 -19.958351, -44.1889559 -19.958437, -44.1893825 -19.9585816, -44.1897582 -19.9587828, -44.1901186 -19.9590453, -44.1912457 -19.9602029, -44.1916575 -19.9606307, -44.1921624 -19.9611588, -44.1925367 -19.9615872, -44.1931832 -19.9622566, -44.1938468 -19.9629343, -44.194089 -19.9631996, -44.1943924 -19.9634141, -44.1946006 -19.9635104, -44.1948789 -19.963599, -44.1957402 -19.9637569, -44.1964094 -19.9638505, -44.1965875 -19.9639188, -44.1967865 -19.9640801, -44.197096 -19.9643572, -44.1972765 -19.964458, -44.1974407 -19.9644824, -44.1976234 -19.9644668, -44.1977654 -19.9644282, -44.1980715 -19.96417, -44.1984541 -19.9638069, -44.1986632 -19.9636002, -44.1988132 -19.9634172, -44.1989542 -19.9632962, -44.1991349 -19.9631081)");
        OGCGeometry lineString = OGCGeometry.fromText("LINESTRING (-44.1247493 -19.9467657, -44.1247979 -19.9468385, -44.1249043 -19.946934, -44.1251096 -19.9470651, -44.1252609 -19.9471383, -44.1254992 -19.947204, -44.1257652 -19.947229, -44.1261292 -19.9471833, -44.1268946 -19.9470098, -44.1276847 -19.9468416, -44.127831 -19.9468143, -44.1282639 -19.9467366, -44.1284569 -19.9467237, -44.1287119 -19.9467261, -44.1289437 -19.9467665, -44.1291499 -19.9468221, -44.1293856 -19.9469396, -44.1298857 -19.9471497, -44.1300908 -19.9472071, -44.1302743 -19.9472331, -44.1305029 -19.9472364, -44.1306498 -19.9472275, -44.1308054 -19.947216, -44.1308553 -19.9472037, -44.1313206 -19.9471394, -44.1317889 -19.9470854, -44.1330422 -19.9468887, -44.1337465 -19.9467083, -44.1339922 -19.9466842, -44.1341506 -19.9466997, -44.1343621 -19.9467226, -44.1345134 -19.9467855, -44.1346494 -19.9468456, -44.1347295 -19.946881, -44.1347988 -19.9469299, -44.1350231 -19.9471131, -44.1355843 -19.9478307, -44.1357802 -19.9480557, -44.1366289 -19.949198, -44.1370384 -19.9497001, -44.137386 -19.9501921, -44.1374113 -19.9502263, -44.1380888 -19.9510925, -44.1381769 -19.9513526, -44.1382509 -19.9516202, -44.1383014 -19.9522136, -44.1383889 -19.9530931, -44.1384227 -19.9538784, -44.1384512 -19.9539653, -44.1384555 -19.9539807, -44.1384901 -19.9541928, -44.1385563 -19.9543859, -44.1386656 -19.9545781, -44.1387339 -19.9546889, -44.1389219 -19.9548661, -44.1391695 -19.9550384, -44.1393672 -19.9551414, -44.1397538 -19.9552208, -44.1401714 -19.9552332, -44.1405656 -19.9551143, -44.1406198 -19.9550853, -44.1407579 -19.9550224, -44.1409029 -19.9549201, -44.1410283 -19.9548257, -44.1413902 -19.9544132, -44.141835 -19.9539274, -44.142268 -19.953484, -44.1427036 -19.9531023, -44.1436229 -19.952259, -44.1437568 -19.9521565, -44.1441783 -19.9517273, -44.144644 -19.9512109, -44.1452538 -19.9505663, -44.1453541 -19.9504774, -44.1458653 -19.9500442, -44.1463563 -19.9496473, -44.1467534 -19.9492812, -44.1470553 -19.9490028, -44.1475804 -19.9485293, -44.1479838 -19.9482096, -44.1485003 -19.9478532, -44.1489451 -19.9477314, -44.1492225 -19.9477024, -44.149453 -19.9476684, -44.149694 -19.9476387, -44.1499556 -19.9475436, -44.1501398 -19.9474234, -44.1502723 -19.9473206, -44.150421 -19.9471473, -44.1505043 -19.9470004, -44.1507664 -19.9462594, -44.150867 -19.9459518, -44.1509225 -19.9457843, -44.1511168 -19.945466, -44.1513601 -19.9452272, -44.1516846 -19.944999, -44.15197 -19.9448738, -44.1525994 -19.9447263, -44.1536614 -19.9444791, -44.1544071 -19.9442671, -44.1548978 -19.9441275, -44.1556247 -19.9438304, -44.1565996 -19.9434083, -44.1570351 -19.9432556, -44.1573142 -19.9432091, -44.1575332 -19.9431645, -44.157931 -19.9431484, -44.1586408 -19.9431504, -44.1593575 -19.9431457, -44.1596498 -19.9431562, -44.1600991 -19.9431475, -44.1602331 -19.9431567, -44.1607926 -19.9432449, -44.1623815 -19.9432765, -44.1628299 -19.9433645, -44.1632475 -19.9435839, -44.1633456 -19.9436559, -44.1636261 -19.9439375, -44.1638186 -19.9442439, -44.1642535 -19.9451781, -44.165178 -19.947156, -44.1652928 -19.9474016, -44.1653074 -19.9474329, -44.1654026 -19.947766, -44.1654774 -19.9481718, -44.1655699 -19.9490241, -44.1656196 -19.9491538, -44.1659735 -19.9499097, -44.1662485 -19.9504925, -44.1662996 -19.9506347, -44.1663574 -19.9512961, -44.1664094 -19.9519273, -44.1664144 -19.9519881, -44.1664799 -19.9526399, -44.1666965 -19.9532586, -44.1671191 -19.9544126, -44.1672019 -19.9545869, -44.1673344 -19.9547603, -44.1675958 -19.9550466, -44.1692349 -19.9567775, -44.1694607 -19.9569284, -44.1718843 -19.9574147, -44.1719167 -19.9574206, -44.1721627 -19.9574748, -44.1723207 -19.9575386, -44.1724439 -19.9575883, -44.1742798 -19.9583293, -44.1748841 -19.9585688, -44.1751118 -19.9586796, -44.1752554 -19.9587769, -44.1752644 -19.9587881, -44.1756052 -19.9592143, -44.1766415 -19.9602689, -44.1774912 -19.9612387, -44.177663 -19.961364, -44.177856 -19.9614494, -44.178034 -19.9615125, -44.1782475 -19.9615423, -44.1785115 -19.9615155, -44.1795404 -19.9610879, -44.1796393 -19.9610759, -44.1798873 -19.9610459, -44.1802404 -19.961036, -44.1804714 -19.9609634, -44.181059 -19.9605365, -44.1815113 -19.9602333, -44.1826712 -19.9594067, -44.1829715 -19.9592551, -44.1837201 -19.9590611, -44.1839277 -19.9590073, -44.1853022 -19.9586512, -44.1856812 -19.9585316, -44.1862915 -19.9584212, -44.1866215 -19.9583494, -44.1867651 -19.9583391, -44.1868852 -19.9583372, -44.1872523 -19.9583313, -44.187823 -19.9583281, -44.1884457 -19.958351, -44.1889559 -19.958437, -44.1893825 -19.9585816, -44.1897582 -19.9587828, -44.1901186 -19.9590453, -44.1912457 -19.9602029, -44.1916575 -19.9606307, -44.1921624 -19.9611588, -44.1925367 -19.9615872, -44.1931832 -19.9622566, -44.1938468 -19.9629343, -44.194089 -19.9631996, -44.1943924 -19.9634141, -44.1946006 -19.9635104, -44.1948789 -19.963599, -44.1957402 -19.9637569, -44.1964094 -19.9638505, -44.1965875 -19.9639188, -44.1967865 -19.9640801, -44.197096 -19.9643572, -44.1972765 -19.964458, -44.1974407 -19.9644824, -44.1976234 -19.9644668, -44.1977654 -19.9644282, -44.1980715 -19.96417, -44.1984541 -19.9638069, -44.1986632 -19.9636002, -44.1988132 -19.9634172, -44.1989542 -19.9632962, -44.1991349 -19.9631081)");
//        point.union(lineString);
//        lineString.union(point);
//        OperatorUnion.local().execute(point.getEsriGeometry(), lineString.getEsriGeometry(), null, null);
        System.out.println(union(point, lineString));
    }

    private OGCGeometry union(OGCGeometry left, OGCGeometry right) {
        String thisType = left.geometryType();
        String anotherType = right.geometryType();
//        if (thisType != anotherType || thisType == OGCConcreteGeometryCollection.TYPE) {
//            //heterogeneous union.
//            //We make a geometry collection, then process to union parts and remove overlaps.
//            ArrayList<OGCGeometry> geoms = new ArrayList<OGCGeometry>();
//            geoms.add(left);
//            geoms.add(right);
//            OGCConcreteGeometryCollection geomCol = new OGCConcreteGeometryCollection(geoms, left.getEsriSpatialReference());
//            return geomCol.flattenAndRemoveOverlaps().reduceFromMulti();
//        }

        OperatorUnion op = (OperatorUnion) OperatorFactoryLocal.getInstance()
                .getOperator(Operator.Type.Union);
        GeometryCursorAppend ap = new GeometryCursorAppend(
                left.getEsriGeometryCursor(), right.getEsriGeometryCursor());
        com.esri.core.geometry.GeometryCursor cursor = op.execute(ap,
                left.getEsriSpatialReference(), null);
        return OGCGeometry.createFromEsriCursor(cursor, left.getEsriSpatialReference());
    }
}
