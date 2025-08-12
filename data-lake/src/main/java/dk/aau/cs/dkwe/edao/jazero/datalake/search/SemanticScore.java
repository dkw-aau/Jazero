package dk.aau.cs.dkwe.edao.jazero.datalake.search;

import dk.aau.cs.dkwe.edao.jazero.datalake.loader.Stats;
import dk.aau.cs.dkwe.edao.jazero.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.similarity.EmbeddingsSimilarity;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.similarity.EntitySimilarity;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityLinking;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.store.EntityTableLink;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.utilities.HungarianAlgorithm;
import dk.aau.cs.dkwe.edao.jazero.datalake.utilities.Utils;

import java.io.File;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SemanticScore extends ScoreBase implements Ranker
{
    public enum SimilarityMeasure
    {
        COSINE("cosine"), EUCLIDEAN("euclidean");

        private final String measure;

        SimilarityMeasure(String measure)
        {
            this.measure = measure;
        }

        @Override
        public String toString()
        {
            return this.measure;
        }

        public boolean equals(SimilarityMeasure other)
        {
            return toString().equals(other.toString());
        }
    }

    private final boolean singleColumnPerQueryEntity;
    private final boolean hungarianAlgorithmSameAlignmentAcrossTuples;
    private final boolean useMaxSimilarityPerColumn;
    private final SimilarityMeasure measure;
    private final EntitySimilarity entitySimilarity;
    private final Map<String, Stats> stats = new HashMap<>();
    private int embeddingCoverageSuccesses, embeddingCoverageFails;
    private Set<String> queryEntitiesMissingCoverage = new HashSet<>();
    private final Object lock = new Object();

    public SemanticScore(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                         boolean singleColumnPerQueryEntity, boolean hungarianAlgorithmSameAlignmentAcrossTuples,
                         boolean useMaxSimilarityPerColumn, SimilarityMeasure measure, EntitySimilarity entitySimilarity)
    {
        super(linker, entityTable, entityTableLink);
        this.singleColumnPerQueryEntity = singleColumnPerQueryEntity;
        this.hungarianAlgorithmSameAlignmentAcrossTuples = hungarianAlgorithmSameAlignmentAcrossTuples;
        this.useMaxSimilarityPerColumn = useMaxSimilarityPerColumn;
        this.measure = measure;
        this.entitySimilarity = entitySimilarity;
    }

    @Override
    public Double score(Table<String> query, File tableFile)
    {
        Table<String> table = TableParser.parse(tableFile);
        Stats.StatBuilder statBuilder = Stats.build();

        if (table == null)
        {
            return null;
        }

        int numEntityMappedRows = 0;    // Number of rows in a table that have at least one cell mapping ot a known entity
        int queryRowsCount = query.rowCount();
        Table<List<Double>> scores = new DynamicTable<>();  // Each cell is a score of the corresponding query cell to the mapped cell in each table row
        int rows = table.rowCount();
        List<List<Integer>> queryRowToColumnMappings = this.singleColumnPerQueryEntity ? getQueryToColumnMapping(query, table) : new ArrayList<>();

        for (int queryRowCounter = 0; queryRowCounter < queryRowsCount; queryRowCounter++)
        {
            int queryRowSize = query.getRow(queryRowCounter).size();
            List<List<Double>> queryRowScores = new ArrayList<>(Collections.nCopies(queryRowSize, new ArrayList<>()));

            for (int row = 0; row < rows; row++)
            {
                Map<Integer, String> columnToEntity = new HashMap<>();
                int columns = table.getRow(row).size();

                for (int column = 0; column < columns; column++)
                {
                    String cellText = table.getRow(row).get(column);
                    String uri = getLinker().mapTo(cellText);

                    if (uri != null)
                    {
                        columnToEntity.put(column, uri);
                    }
                }

                if (columnToEntity.isEmpty())   // Compute similarity vectors only for rows that map to at least one entity
                {
                    continue;
                }

                numEntityMappedRows++;

                if (!(this.entitySimilarity instanceof EmbeddingsSimilarity) ||
                        hasEmbeddingCoverage(query.getRow(queryRowCounter), columnToEntity, queryRowToColumnMappings, queryRowCounter))
                {
                    for (int queryColumn = 0; queryColumn < queryRowSize; queryColumn++)
                    {
                        String queryEntity = query.getRow(queryRowCounter).get(queryColumn);
                        double bestSimScore = 0.0;

                        if (this.singleColumnPerQueryEntity)
                        {
                            int assignedColumn = queryRowToColumnMappings.get(queryRowCounter).get(queryColumn);

                            if (columnToEntity.containsKey(assignedColumn))
                            {
                                bestSimScore = this.entitySimilarity.similarity(queryEntity, columnToEntity.get(assignedColumn));
                            }
                        }

                        else
                        {
                            for (String rowEntity : columnToEntity.values()) // Loop over each entity in the table row
                            {
                                double simScore = this.entitySimilarity.similarity(queryEntity, rowEntity);
                                bestSimScore = Math.max(bestSimScore, simScore);
                            }
                        }

                        queryRowScores.get(queryColumn).add(bestSimScore);
                    }
                }
            }

            scores.addRow(new Table.Row<>(queryRowScores));
        }

        // Update Statistics
        statBuilder.entityMappedRows(numEntityMappedRows);
        statBuilder.fractionOfEntityMappedRows((double) numEntityMappedRows / rows);
        Double score = aggregateTableSimilarities(query, scores, statBuilder);
        this.stats.put(tableFile.getName(), statBuilder.finish());

        return score;
    }

    /**
     * Initialize multi-dimensional array indexed by (tupleID, entityID, columnID) mapping to the
     * aggregated score for that query entity with respect to the column
     */
    private List<List<Integer>> getQueryToColumnMapping(Table<String> query, Table<String> table)
    {
        int tableRows = table.rowCount(), queryRows = query.rowCount();
        List<List<List<Double>>> entityToColumnScore = new ArrayList<>(tableRows);

        for (int row = 0; row < queryRows; row++)
        {
            int rowSize = query.getRow(row).size();
            List<List<Double>> queryRowScores = new ArrayList<>(rowSize);

            for (int rowEntity = 0; rowEntity < rowSize; rowEntity++)
            {
                queryRowScores.add(new ArrayList<>(Collections.nCopies(table.columnCount(), 0.0)));
            }

            entityToColumnScore.add(queryRowScores);
        }

        for (int tableRow = 0; tableRow < tableRows; tableRow++)
        {
            int colCounter = 0;
            Table.Row<String> row = table.getRow(tableRow);

            for (String cell : row)
            {
                String curEntity = getLinker().mapTo(cell);

                if (curEntity != null)
                {
                    for (int queryRow = 0; queryRow < queryRows; queryRow++)    // Loop over each query tuple and each entity in a tuple and compute a score between the query entity and 'curEntity'
                    {
                        int queryRowCells = query.getRow(queryRow).size();

                        for (int queryEntityCounter = 0; queryEntityCounter < queryRowCells; queryEntityCounter++)
                        {
                            String queryEntity = query.getRow(queryRow).get(queryEntityCounter);
                            double score = this.entitySimilarity.similarity(queryEntity, curEntity);
                            entityToColumnScore.get(queryRow).get(queryEntityCounter).set(colCounter, entityToColumnScore.get(queryRow).get(queryEntityCounter).get(colCounter) + score);
                        }
                    }
                }

                colCounter++;
            }
        }

        List<List<Integer>> tupleToColumnMappings = getBestMatchFromScores(query, entityToColumnScore); // Find the best mapping between a query entity and a column for each query tuple

        if (this.hungarianAlgorithmSameAlignmentAcrossTuples)
        {
            for (int row = 1; row < tupleToColumnMappings.size(); row++)    // Modify tupleToColumnMappings so that the same column alignments are used across all query tuples
            {
                tupleToColumnMappings.set(row, tupleToColumnMappings.get(0));
            }
        }

        return tupleToColumnMappings;
    }

    private boolean hasEmbeddingCoverage(Table.Row<String> queryRow, Map<Integer, String> columnToEntity,
                                         List<List<Integer>> tupleToColumnMappings, Integer queryRowIndex)
    {
        for (int i = 0; i < queryRow.size(); i++)   // Ensure that all query entities have an embedding
        {
            if (!entityExists(queryRow.get(i)))
            {
                synchronized (this.lock)
                {
                    this.embeddingCoverageFails++;
                    this.queryEntitiesMissingCoverage.add(queryRow.get(i));
                    return false;
                }
            }
        }

        // If `singleColumnPerQueryEntity` is true then ensure that all row entities that are
        // in the chosen columns (i.e. tupleToColumnMappings.get(queryTupleID) ) need to be mappable
        List<String> relevantRowEntities = new ArrayList<>();

        if (this.singleColumnPerQueryEntity)
        {
            for (int assignedColumn : tupleToColumnMappings.get(queryRowIndex))
            {
                if (columnToEntity.containsKey(assignedColumn))
                {
                    relevantRowEntities.add(columnToEntity.get(assignedColumn));
                }
            }
        }

        else    // All entities in `rowEntities` are relevant
        {
            relevantRowEntities = new ArrayList<>(columnToEntity.values());
        }

        for (String rowEnt : relevantRowEntities)   // Loop over all relevant row entities and ensure there is a pre-trained embedding mapping for each one
        {
            if (!entityExists(rowEnt))
            {
                synchronized (this.lock)
                {
                    this.embeddingCoverageFails++;
                    return false;
                }
            }
        }

        if (relevantRowEntities.isEmpty())
        {
            synchronized (this.lock)
            {
                this.embeddingCoverageFails++;
                return false;
            }
        }

        synchronized (this.lock)
        {
            this.embeddingCoverageSuccesses++;
        }

        return true;
    }

    /**
     * Checks for existence of entity in database of entity embeddings
     * @param entity Entity to check
     * @return true if the entity exists in the embeddings database
     */
    private boolean entityExists(String entity)
    {
        try
        {
            Id id = getLinker().uriLookup(entity);
            return id != null && getEntityTable().find(id) != null;
        }

        catch (IllegalArgumentException exc)
        {
            return false;
        }
    }

    /**
     * Mapping of the matched columnIDs for each entity in each query tuple
     * Indexed by (tupleID, entityID) mapping to the columnID. If a columnID is -1 then that entity is not chosen for assignment
     * @param query Query table
     * @param entityToColumnScore Column score per entity
     * @return Best match from given scores
     */
    private List<List<Integer>> getBestMatchFromScores(Table<String> query, List<List<List<Double>>> entityToColumnScore)
    {
        List<List<Integer>> tupleToColumnMappings = new ArrayList<>();

        for (int row = 0; row < query.rowCount(); row++)
        {
            // 2-D array where each row is composed of the negative column relevance scores for a given entity in the query tuple
            // Taken from: https://stackoverflow.com/questions/10043209/convert-arraylist-into-2d-array-containing-varying-lengths-of-arrays
            double[][] scoresMatrix = entityToColumnScore.get(row).stream().map(u -> u.stream().mapToDouble(i -> -1 * i).toArray()).toArray(double[][]::new);

            // Run the Hungarian Algorithm on the scoresMatrix
            // If there are less columns that rows, some rows (i.e. query entities) will not be assigned to a column.
            // More specifically they will be assigned to a column id of -1
            HungarianAlgorithm ha = new HungarianAlgorithm(scoresMatrix);
            int[] assignmentArray = ha.execute();
            List<Integer> assignmentList = Arrays.stream(assignmentArray).boxed().collect(Collectors.toList());
            tupleToColumnMappings.add(assignmentList);
        }

        return tupleToColumnMappings;
    }

    /**
     * Aggregates scores into a single table score
     * @param query Query table
     * @param scores Table of query entity scores
     * @param statBuilder Statistics
     * @return Single score of table
     */
    private Double aggregateTableSimilarities(Table<String> query, Table<List<Double>> scores, Stats.StatBuilder statBuilder)
    {
        // Compute the weighted vector (i.e. considers IDF scores of query entities) for each query tuple
        Map<Integer, List<Double>> queryRowToWeightVector = new HashMap<>();
        int queryRows = query.rowCount();

        for (int queryRow = 0; queryRow < queryRows; queryRow++)
        {
            int rowSize = query.getRow(queryRow).size();
            List<Double> curRowIDFScores  = new ArrayList<>(rowSize);

            for (int column = 0; column < rowSize; column++)
            {
                Id entityId = getLinker().uriLookup(query.getRow(queryRow).get(column));
                Entity entity = getEntityTable().find(entityId);

                if (entity != null)
                {
                    curRowIDFScores.add(entity.getIDF());
                }

                else
                {
                    curRowIDFScores.add(1.0);
                }
            }

            queryRowToWeightVector.put(queryRow, Utils.normalizeVector(curRowIDFScores));
        }

        // Compute a score for the current file with respect to each query tuple
        // The score takes into account the weight vector associated with each tuple
        Map<Integer, Double> tupleIDToScore = new HashMap<>();
        List<List<Double>> queryRowVectors = new ArrayList<>();    // 2D List mapping each tupleID to the similarity scores chosen across the aligned columns

        for (int queryRow = 0; queryRow < queryRows; queryRow++)
        {
            if (tableRowExists(scores.getRow(queryRow), l -> !l.isEmpty())) // Ensure that the current query row has at least one similarity vector with some row
            {
                List<Double> curQueryRowVec;

                if (this.useMaxSimilarityPerColumn)  // Use the maximum similarity score per column as the tuple vector
                {
                    curQueryRowVec = Utils.getMaxPerColumnVector(scores.getRow(queryRow));
                }

                else
                {
                    curQueryRowVec = Utils.getAverageVector(scores.getRow(queryRow));
                }

                List<Double> identityVector = new ArrayList<>(Collections.nCopies(curQueryRowVec.size(), 1.0));
                double score = 0.0;

                if (this.measure == SimilarityMeasure.COSINE)   // Note: Cosine similarity doesn't make sense if we are operating in a vector similarity space
                {
                    score = Utils.cosineSimilarity(curQueryRowVec, identityVector);
                }

                else if (this.measure == SimilarityMeasure.EUCLIDEAN)   // Perform weighted euclidean distance between the `curTupleVec` and `identity
                {
                    score = Utils.euclideanDistance(curQueryRowVec, identityVector, queryRowToWeightVector.get(queryRow));
                    score = 1 / (score + 1);    // Convert euclidean distance to similarity, high similarity (i.e. close to 1) means euclidean distance is small
                }

                tupleIDToScore.put(queryRow, score);
                queryRowVectors.add(curQueryRowVec);  // Update the tupleVectors array
            }

            else
            {
                tupleIDToScore.put(queryRow, 0.0);
            }
        }

        // TODO: Each tuple currently weighted equally. Maybe add extra weighting per tuple when taking average?
        if (!tupleIDToScore.isEmpty())  // Get a single score for the current filename that is averaged across all query tuple scores
        {
            List<Double> queryRowScores = new ArrayList<>(tupleIDToScore.values());
            statBuilder.queryRowScores(queryRowScores);
            statBuilder.queryRowVectors(queryRowVectors);
            return Utils.getAverageOfVector(queryRowScores);
        }

        return 0.0;
    }

    private <E> boolean tableRowExists(Table.Row<E> row, Predicate<E> function)
    {
        for (int i = 0; i < row.size(); i++)
        {
            if (function.test(row.get(i)))
                return true;
        }

        return false;
    }

    public Map<String, Stats> getStats()
    {
        return this.stats;
    }

    public int getEmbeddingComparisons()
    {
        return this.entitySimilarity instanceof EmbeddingsSimilarity ?
                ((EmbeddingsSimilarity) this.entitySimilarity).getEmbeddingComparisons() : 0;
    }

    public int getNonEmbeddingComparisons()
    {
        return this.entitySimilarity instanceof EmbeddingsSimilarity ?
                ((EmbeddingsSimilarity) this.entitySimilarity).getNonEmbeddingComparisons() : 0;
    }

    public int getEmbeddingCoverageSuccesses()
    {
        return this.embeddingCoverageSuccesses;
    }

    public int getEmbeddingCoverageFails()
    {
        return this.embeddingCoverageFails;
    }

    public Set<String> getQueryEntitiesMissingCoverage()
    {
        return this.queryEntitiesMissingCoverage;
    }
}
