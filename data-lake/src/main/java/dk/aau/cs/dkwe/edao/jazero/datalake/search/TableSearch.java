package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.loader.Stats;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.jazero.storagelayer.StorageHandler;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class TableSearch extends AbstractSearch
{
    private final int topK, threads;
    private long elapsed = -1, parsedTables;
    private double reduction = 0.0;
    private final StorageHandler storage;
    private final Ranker ranker;
    private Prefilter prefilter;

    public TableSearch(StorageHandler tableStorage, Ranker ranker, int topK, int threads)
    {
        this.topK = topK;
        this.threads = threads;
        this.storage = tableStorage;
        this.ranker = ranker;
    }

    public TableSearch(StorageHandler tableStorage, Ranker ranker, int topK, int threads, Prefilter prefilter)
    {
        this(tableStorage, ranker, topK, threads);
        this.prefilter = prefilter;
    }

    private Set<File> prefilterSearchSpace(Table<String> query)
    {
        int initialSize = this.storage.count();
        Iterator<Pair<File, Double>> res = this.prefilter.search(query).getResults();
        Set<String> tableNames = new HashSet<>();

        while (res.hasNext())
        {
            tableNames.add(res.next().first().getName());
        }

        this.reduction = initialSize > 0 ? (1 - ((double) tableNames.size() / initialSize)) : 0;
        return this.storage.elements(f -> tableNames.contains(f.getName()));    // We have to iterate like this because we don't know how the storage type refers to a file absolutely
    }

    /**
     * Entry point for analogous search
     * @param query Input table query
     * @return Top-K ranked result container
     */
    @Override
    protected Result abstractSearch(Table<String> query)
    {
        try
        {
            long start = System.nanoTime();
            ExecutorService threadPool = Executors.newFixedThreadPool(this.threads);
            List<Future<Pair<File, Double>>> parsed = new ArrayList<>(this.storage.count());
            Iterator<File> tableFiles = this.prefilter != null ? prefilterSearchSpace(query).iterator() : this.storage.iterator();
            int tableCount = 0;

            if (this.prefilter != null)
            {
                Logger.log(Logger.Level.INFO, "Pre-filtered corpus in " + this.prefilter.elapsedNanoSeconds() + "ns");
            }

            while (tableFiles.hasNext())
            {
                File tableFile = tableFiles.next();
                Future<Pair<File, Double>> future = threadPool.submit(() -> new Pair<>(tableFile, this.ranker.score(query, tableFile)));
                parsed.add(future);
                tableCount++;
            }

            long done = 1, prev = 0;
            Logger.log(Logger.Level.INFO, "Processing " + tableCount + " files.");

            while (done < tableCount)
            {
                done = parsed.stream().filter(Future::isDone).count();

                if (done - prev >= 100)
                {
                    Logger.log(Logger.Level.INFO, "Processed " + done + "/" + tableCount + " files...");
                    prev = done;
                }
            }

            List<Pair<File, Double>> scores = new ArrayList<>();
            long parsedTables = parsed.stream().filter(f -> {
                try
                {
                    Pair<File, Double> tableScore = f.get();

                    if (tableScore != null)
                    {
                        scores.add(tableScore);
                        return true;
                    }

                    return false;
                }

                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e.getMessage());
                }
            }).count();

            Map<String, Stats> stats = new HashMap<>();
            this.elapsed = System.nanoTime() - start;
            this.parsedTables = parsedTables;
            Logger.log(Logger.Level.INFO, "A total of " + parsedTables + " tables were parsed.");
            Logger.log(Logger.Level.INFO, "Elapsed time: " + this.elapsed / 1e9 + " seconds\n");

            if (this.prefilter != null)
            {
                this.elapsed += this.prefilter.elapsedNanoSeconds();
            }

            if (this.ranker instanceof SemanticScore)
            {
                SemanticScore semanticRanker = (SemanticScore) this.ranker;
                stats = semanticRanker.getStats();

                Logger.log(Logger.Level.INFO, "A total of " + semanticRanker.getEmbeddingComparisons() + " entity comparisons were made using embeddings.");
                Logger.log(Logger.Level.INFO, "A total of " + semanticRanker.getNonEmbeddingComparisons() + " entity comparisons cannot be made due to lack of embeddings.");

                double percentage = (semanticRanker.getEmbeddingComparisons() / ((double) semanticRanker.getNonEmbeddingComparisons() + semanticRanker.getEmbeddingComparisons())) * 100;
                Logger.log(Logger.Level.INFO, percentage + "% of required entity comparisons were made using embeddings.\n");
                Logger.log(Logger.Level.INFO, "Embedding Coverage successes: " + semanticRanker.getEmbeddingCoverageSuccesses());
                Logger.log(Logger.Level.INFO, "Embedding Coverage failures: " + semanticRanker.getEmbeddingCoverageFails());
                Logger.log(Logger.Level.INFO, "Embedding Coverage Success Rate: " + (double) semanticRanker.getEmbeddingCoverageSuccesses() /
                        (semanticRanker.getEmbeddingCoverageSuccesses() + semanticRanker.getEmbeddingCoverageFails()));
                Logger.log(Logger.Level.INFO, "Query Entities with missing embedding coverage: " + semanticRanker.getQueryEntitiesMissingCoverage() + "\n");
            }

            return new Result(this.topK, scores, this.elapsed, this.reduction, stats);
        }

        catch (RuntimeException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }

    public long getParsedTables()
    {
        return this.parsedTables;
    }

    public double getReduction()
    {
        return this.reduction;
    }
}
