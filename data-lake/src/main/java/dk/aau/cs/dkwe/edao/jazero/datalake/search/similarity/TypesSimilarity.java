package dk.aau.cs.dkwe.edao.jazero.datalake.search.similarity;

import dk.aau.cs.dkwe.edao.jazero.datalake.similarity.JaccardSimilarity;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Type;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TypesSimilarity implements EntitySimilarity
{
    private final boolean weighted;
    private final EntityLinking linker;
    private final EntityTable entityTable;

    public TypesSimilarity(boolean weighted, EntityLinking linker, EntityTable entityTable)
    {
        this.weighted = weighted;
        this.linker = linker;
        this.entityTable = entityTable;
    }

    @Override
    public double similarity(String entity1, String entity2)
    {
        Set<Type> types1 = new HashSet<>(), types2 = new HashSet<>();
        Id id1 = this.linker.uriLookup(entity1), id2 = this.linker.uriLookup(entity2);

        if (this.entityTable.contains(id1))
        {
            Entity entity = this.entityTable.find(id1);
            types1 = new HashSet<>(entity.getTypes());
        }

        if (this.entityTable.contains(id2))
        {
            Entity entity = this.entityTable.find(id2);
            types2 = new HashSet<>(entity.getTypes());
        }

        double jaccardScore = 0.0;

        if (this.weighted)
        {
            Set<Pair<Type, Double>> weights = types1.stream().map(t -> new Pair<>(t, t.getIdf())).collect(Collectors.toSet());
            weights.addAll(types2.stream().map(t -> new Pair<>(t, t.getIdf())).collect(Collectors.toSet()));
            weights = weights.stream().filter(p -> p.second() >= 0).collect(Collectors.toSet());
            jaccardScore = JaccardSimilarity.make(types1, types2, weights).similarity();
        }

        else
        {
            jaccardScore = JaccardSimilarity.make(types1, types2).similarity();
        }

        return Math.min(0.95, jaccardScore);
    }
}
