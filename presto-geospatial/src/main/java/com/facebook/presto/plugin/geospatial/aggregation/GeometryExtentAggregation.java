package com.facebook.presto.plugin.geospatial.aggregation;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import com.esri.core.geometry.Envelope;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.geospatial.serde.GeometrySerde.deserializeEnvelope;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;

@AggregationFunction("ST_Extent")
public class GeometryExtentAggregation
{
    private static final class ExtentAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ExtentAggregationState.class).instanceSize();

        private double xMin = Double.POSITIVE_INFINITY;
        private double yMin = Double.POSITIVE_INFINITY;
        private double xMax = Double.NEGATIVE_INFINITY;
        private double yMax = Double.NEGATIVE_INFINITY;

        public void extend(Envelope otherEnvelope)
        {
            if (otherEnvelope.isEmpty()) {
                return;
            }

            xMin = Math.min(xMin, otherEnvelope.getXMin());
            yMin = Math.min(yMin, otherEnvelope.getYMin());
            xMax = Math.max(xMax, otherEnvelope.getXMax());
            yMax = Math.max(yMax, otherEnvelope.getYMax());
        }

        public long getEstimatedSize()
        {
            return INSTANCE_SIZE;
        }

        public boolean isEmpty()
        {
            return Double.isInfinite(xMin)
                    || Double.isInfinite(yMin)
                    || Double.isInfinite(xMax)
                    || Double.isInfinite(yMax);
        }
    }

    private GeometryExtentAggregation() {}

    @InputFunction
    public static void extend(@AggregationState ExtentAggregationState state, @SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        state.extend(deserializeEnvelope(input));
    }

    @CombineFunction
    public static void combine(@AggregationState ExtentAggregationState state, @AggregationState ExtentAggregationState otherState)
    {
        state.xMin = Math.min(state.xMin, otherState.xMin);
        state.yMin = Math.min(state.yMin, otherState.yMin);
        state.xMax = Math.max(state.xMax, otherState.xMax);
        state.yMax = Math.max(state.yMax, otherState.yMax);
    }

    @OutputFunction(GEOMETRY_TYPE_NAME)
    public static void output(@AggregationState ExtentAggregationState state, BlockBuilder out)
    {

    }
}
