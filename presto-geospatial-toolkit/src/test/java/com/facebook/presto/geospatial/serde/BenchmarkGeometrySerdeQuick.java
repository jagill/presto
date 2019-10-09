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

import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static com.facebook.presto.geospatial.serde.BenchmarkGeometrySerializationData.POINT;
import static com.facebook.presto.geospatial.serde.BenchmarkGeometrySerializationData.readResource;
import static com.facebook.presto.geospatial.serde.GeometrySerde.serialize;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 2, timeUnit = SECONDS)
//@Measurement(iterations = 1, time = 600, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = SECONDS)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkGeometrySerdeQuick
{
    // Point
    @Benchmark
    public Object pointFromTextOgc(BenchmarkData data)
    {
        return OGCGeometry.fromText(data.pointWkt);
    }

    @Benchmark
    public Object pointFromTextJts(BenchmarkData data)
    {
        return jtsFromText(data.pointWkt);
    }

//    @Benchmark
//    public Object serializePointOgc(BenchmarkData data)
//    {
//        return GeometrySerde.serialize(data.pointOgc);
//    }
//
//    @Benchmark
//    public Object serializePointJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.serialize(data.pointJts);
//    }
//
//    @Benchmark
//    public Object deserializePointOgc(BenchmarkData data)
//    {
//        return GeometrySerde.deserialize(data.pointSerialized);
//    }
//
//    @Benchmark
//    public Object deserializePointJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.deserialize(data.pointSerialized);
//    }

    // LineString
    @Benchmark
    public Object lineStringFromTextOgc(BenchmarkData data)
    {
        return OGCGeometry.fromText(data.lineStringWkt);
    }

    @Benchmark
    public Object lineStringFromTextJts(BenchmarkData data)
    {
        return jtsFromText(data.lineStringWkt);
    }

//    @Benchmark
//    public Object serializeLineStringOgc(BenchmarkData data)
//    {
//        return GeometrySerde.serialize(data.lineStringOgc);
//    }
//
//    @Benchmark
//    public Object serializeLineStringJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.serialize(data.lineStringJts);
//    }
//
//    @Benchmark
//    public Object deserializeLineStringOgc(BenchmarkData data)
//    {
//        return GeometrySerde.deserialize(data.lineStringSerialized);
//    }
//
//    @Benchmark
//    public Object deserializeLineStringJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.deserialize(data.lineStringSerialized);
//    }

    // MultiPolygon
    @Benchmark
    public Object multiPolygonFromTextOgc(BenchmarkData data)
    {
        return OGCGeometry.fromText(data.multiPolygonWkt);
    }

    @Benchmark
    public Object multiPolygonFromTextJts(BenchmarkData data)
    {
        return jtsFromText(data.multiPolygonWkt);
    }

//    @Benchmark
//    public Object serializeMultiPolygonOgc(BenchmarkData data)
//    {
//        return GeometrySerde.serialize(data.multiPolygonOgc);
//    }
//
//    @Benchmark
//    public Object serializeMultiPolygonJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.serialize(data.multiPolygonJts);
//    }
//
//    @Benchmark
//    public Object deserializeMultiPolygonOgc(BenchmarkData data)
//    {
//        return GeometrySerde.deserialize(data.multiPolygonSerialized);
//    }
//
//    @Benchmark
//    public Object deserializeMultiPolygonJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.deserialize(data.multiPolygonSerialized);
//    }

    // GeometryCollection
    @Benchmark
    public Object geometryCollectionFromTextOgc(BenchmarkData data)
    {
        return OGCGeometry.fromText(data.geometryCollectionWkt);
    }

    @Benchmark
    public Object geometryCollectionFromTextJts(BenchmarkData data)
    {
        return jtsFromText(data.geometryCollectionWkt);
    }

//    @Benchmark
//    public Object serializeGeometryCollectionOgc(BenchmarkData data)
//    {
//        return GeometrySerde.serialize(data.geometryCollectionOgc);
//    }
//
//    @Benchmark
//    public Object serializeGeometryCollectionJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.serialize(data.geometryCollectionJts);
//    }
//
//    @Benchmark
//    public Object deserializeGeometryCollectionOgc(BenchmarkData data)
//    {
//        return GeometrySerde.deserialize(data.geometryCollectionSerialized);
//    }
//
//    @Benchmark
//    public Object deserializeGeometryCollectionJts(BenchmarkData data)
//    {
//        return JtsGeometrySerde.deserialize(data.geometryCollectionSerialized);
//    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private GeometryFactory geometryFactory = new GeometryFactory(new PackedCoordinateSequenceFactory());

        private String pointWkt;
        private OGCGeometry pointOgc;
        private Geometry pointJts;
        private Slice pointSerialized;

        private String lineStringWkt;
        private OGCGeometry lineStringOgc;
        private Geometry lineStringJts;
        private Slice lineStringSerialized;

        private String multiPolygonWkt;
        private OGCGeometry multiPolygonOgc;
        private Geometry multiPolygonJts;
        private Slice multiPolygonSerialized;

        private String geometryCollectionWkt;
        private OGCGeometry geometryCollectionOgc;
        private Geometry geometryCollectionJts;
        private Slice geometryCollectionSerialized;


        @Setup
        public void setup()
        {
            pointWkt = POINT;
            pointOgc = OGCGeometry.fromText(POINT);
            pointJts = jtsFromText(POINT);
            pointSerialized = serialize(pointOgc);

            lineStringWkt = readResource("complex-linestring.txt");
            lineStringOgc = OGCGeometry.fromText(lineStringWkt);
            lineStringJts = jtsFromText(lineStringWkt);
            lineStringSerialized = serialize(lineStringOgc);

            multiPolygonWkt = readResource("complex-multipolygon.txt");
            multiPolygonOgc = OGCGeometry.fromText(multiPolygonWkt);
            multiPolygonJts = jtsFromText(multiPolygonWkt);
            multiPolygonSerialized = serialize(multiPolygonOgc);

            geometryCollectionWkt = readResource("complex-multipolygon.txt");
            geometryCollectionOgc = OGCGeometry.fromText(geometryCollectionWkt);
            geometryCollectionJts = jtsFromText(geometryCollectionWkt);
            geometryCollectionSerialized = serialize(geometryCollectionOgc);
        }

    }

    private static Geometry jtsFromText(String wkt)
    {
        try {
            return new WKTReader().read(wkt);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGeometrySerdeQuick.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

}