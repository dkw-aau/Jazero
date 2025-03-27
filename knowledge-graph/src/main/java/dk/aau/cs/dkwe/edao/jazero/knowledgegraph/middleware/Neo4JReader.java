package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware;

import dk.aau.cs.dkwe.edao.jazero.datalake.loader.IndexIO;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.jazero.knowledgegraph.connector.Neo4jEndpoint;
import org.neo4j.driver.Record;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reads KG by exporting from Neo4J instance
 * Output is stored in kg.ttl
 * <p>
 * WARNING
 *      Using the RDF export procedure in Neo4J is an experimental feature which may change in the future
 *      - <a href="https://neo4j.com/labs/neosemantics/tutorial/#_using_the_cypher_n10s_rdf_export_procedure">...</a>
 */
public class Neo4JReader extends Neo4JHandler implements IndexIO
{
    private final Neo4jEndpoint endpoint;
    private Set<Record> entityLabels = null;

    public Neo4JReader(Neo4jEndpoint endpoint)
    {
        this.endpoint = endpoint;
    }

    @Override
    public void performIO() throws IOException
    {}

    /**
     * Returns all entities and some of their textual literal objects
     * @return In-memory sub-graph
     */
    public Map<String, Set<String>> subGraph()
    {
        Map<String, Set<String>> subKG = new HashMap<>();
        Set<String> entities = this.endpoint.getEntities();

        for (String entity : entities)
        {
            Set<String> objects = new HashSet<>();
            String label = this.endpoint.getLabel(entity);
            String caption = this.endpoint.getCaption(entity);

            if (label != null)
            {
                objects.add(label);
            }

            if (caption != null)
            {
                objects.add(caption);
            }

            subKG.put(entity, objects);
        }

        return subKG;
    }

    public List<Type> entityTypes(Entity entity)
    {
        List<String> typesStr = this.endpoint.searchTypes(entity.getUri());
        return typesStr.stream().map(Type::new).collect(Collectors.toList());
    }

    public List<String> entityLabels(Entity entity)
    {
        if (this.entityLabels == null)
        {
            this.entityLabels = this.endpoint.entityLabels();
        }

        String entityUri = entity.getUri();
        List<String> labels = new ArrayList<>();
        this.entityLabels.forEach(record -> {
            if (!record.get("label").isNull() && record.get("uri").asString().equals(entityUri))
            {
                labels.add(record.get("label").asString());
            }
        });

        return labels;
    }

    public List<String> entityPredicates(Entity entity)
    {
        return this.endpoint.searchPredicates(entity.getUri());
    }

    public long size()
    {
        return this.endpoint.getNumNodes();
    }
}
