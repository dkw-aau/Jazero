package dk.aau.cs.dkwe.edao.jazero.datalake.search.similarity;

import dk.aau.cs.dkwe.edao.jazero.datalake.similarity.JaccardSimilarity;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;

import java.util.HashSet;
import java.util.Set;

public class PredicatesSimilarity implements EntitySimilarity
{
    private final EntityLinking linker;
    private final EntityTable entityTable;

    public PredicatesSimilarity(EntityLinking linker, EntityTable entityTable)
    {
        this.linker = linker;
        this.entityTable = entityTable;
    }

    @Override
    public double similarity(String entity1, String entity2)
    {
        Set<String> predicates1 = new HashSet<>(), predicates2 = new HashSet<>();
        Id id1 = this.linker.uriLookup(entity1), id2 = this.linker.uriLookup(entity2);

        if (this.entityTable.contains(id1))
        {
            Entity entity = this.entityTable.find(id1);
            predicates1 = new HashSet<>(entity.getPredicates());
        }

        if (this.entityTable.contains(id2))
        {
            Entity entity = this.entityTable.find(id2);
            predicates2 = new HashSet<>(entity.getPredicates());
        }

        double jaccardScore = JaccardSimilarity.make(predicates1, predicates2).similarity();
        return Math.min(0.95, jaccardScore);
    }
}
