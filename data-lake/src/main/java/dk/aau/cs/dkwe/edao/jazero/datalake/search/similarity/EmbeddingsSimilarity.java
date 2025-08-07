package dk.aau.cs.dkwe.edao.jazero.datalake.search.similarity;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Embedding;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;

public class EmbeddingsSimilarity implements EntitySimilarity
{
    public enum CosineType
    {
        NORM, ABSOLUTE, ANGULAR;
    }

    private final EntityLinking linker;
    private final EntityTable entityTable;
    private final CosineType cosineType;
    private final Object lock = new Object();
    private int embeddingComparisons = 0, nonEmbeddingComparisons = 0;

    public EmbeddingsSimilarity(CosineType type, EntityLinking linker, EntityTable entityTable)
    {
        this.cosineType = type;
        this.linker = linker;
        this.entityTable = entityTable;
    }

    @Override
    public double similarity(String entity1, String entity2)
    {
        Id id1 = this.linker.uriLookup(entity1), id2 = this.linker.uriLookup(entity2);

        if (id1 == null || id2 == null)
        {
            synchronized (this.lock)
            {
                this.nonEmbeddingComparisons++;
                return 0.0;
            }
        }

        Embedding embedding1 = this.entityTable.find(id1).getEmbedding(),
                embedding2 = this.entityTable.find(id2).getEmbedding();

        if (embedding1 == null || embedding2 == null)
        {
            synchronized (this.lock)
            {
                this.nonEmbeddingComparisons++;
                return 0.0;
            }
        }

        double cosineSimilarity = embedding1.cosine(embedding2), similarityScore = 0.0;

        if (this.cosineType == CosineType.NORM)
        {
            similarityScore = (cosineSimilarity + 1.0) / 2.0;
        }

        else if (this.cosineType == CosineType.ABSOLUTE)
        {
            similarityScore = Math.abs(cosineSimilarity);
        }

        else
        {
            similarityScore = 1 - Math.acos(cosineSimilarity) / Math.PI;
        }

        synchronized (this.lock)
        {
            this.embeddingComparisons++;
        }

        return similarityScore;
    }

    public int getEmbeddingComparisons()
    {
        return this.embeddingComparisons;
    }

    public int getNonEmbeddingComparisons()
    {
        return this.nonEmbeddingComparisons;
    }
}
