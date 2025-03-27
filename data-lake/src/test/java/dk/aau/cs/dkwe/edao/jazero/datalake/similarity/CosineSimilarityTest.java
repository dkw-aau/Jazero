package dk.aau.cs.dkwe.edao.jazero.datalake.similarity;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CosineSimilarityTest
{
    private List<Double> vec1 = List.of(1.0, 2.0, 3.0), vec2 = List.of(3.0, 2.0, 1.0), vec3 = List.of(3.0, 3.0, 3.0);

    @Test
    public void testCosine()
    {
        assertEquals(0.7142, CosineSimilarity.make(this.vec1, this.vec2).similarity(), 0.001);
        assertEquals(0.9258, CosineSimilarity.make(this.vec1, this.vec3).similarity(), 0.001);
        assertEquals(0.9258, CosineSimilarity.make(this.vec2, this.vec3).similarity(), 0.001);
    }
}
