package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.type.codec.CqlVectorCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

public class StringToVectorCodecTest {

    private final ArrayList<Float> values = Lists.newArrayList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f);
    private final CqlVector vector = CqlVector.builder().addAll(values).build();
    private final CqlVectorCodec vectorCodec = new CqlVectorCodec(
            new CqlVectorType(DataTypes.FLOAT, 5),
            TypeCodecs.FLOAT);

    private final StringToVectorCodec dsbulkCodec = new StringToVectorCodec(
            vectorCodec,
            Lists.newArrayList("NULL"));

    @Test
    void should_convert_from_valid_external() {
        assertThat(dsbulkCodec)
                .convertsFromExternal(vectorCodec.format(vector)) // standard pattern
                .toInternal(vector)
                .convertsFromExternal("")
                .toInternal(null)
                .convertsFromExternal(null)
                .toInternal(null)
                .convertsFromExternal("NULL")
                .toInternal(null);
    }

    @Test
    void should_convert_from_valid_internal() {
        assertThat(dsbulkCodec)
                .convertsFromInternal(vector)
                .toExternal(vectorCodec.format(vector))
                .convertsFromInternal(null)
                .toExternal("NULL");
    }

    @Test
    void should_not_convert_from_invalid_external() {
        // Too few values to match dimensions
        ArrayList<Float> tooMany = Lists.newArrayList(values);
        tooMany.add(6.6f);
        ArrayList<Float> tooFew = Lists.newArrayList(values);
        tooFew.remove(0);

        assertThat(dsbulkCodec)
                .cannotConvertFromExternal(CqlVector.builder().addAll(tooMany).build())
                .cannotConvertFromExternal(CqlVector.builder().addAll(tooFew).build())
                .cannotConvertFromExternal("not a valid vector");
    }
}
