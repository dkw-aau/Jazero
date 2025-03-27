package dk.aau.cs.dkwe.edao.jazero.knowledgegraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Logger;
import dk.aau.cs.dkwe.edao.jazero.knowledgegraph.connector.Neo4jEndpoint;
import dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware.*;
import dk.aau.cs.dkwe.edao.jazero.knowledgegraph.search.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RestController
public class KnowledgeGraph implements WebServerFactoryCustomizer<ConfigurableWebServerFactory>
{
    private static Neo4jEndpoint endpoint;
    private static final String KG_DIR = "/home/kg/";
    private static LuceneIndex search;

    @Override
    public void customize(ConfigurableWebServerFactory factory)
    {
        factory.setPort(Configuration.getEKGManagerPort());
    }

    public static void main(String[] args)
    {
        try
        {
            if (!LuceneFactory.isBuild())
            {
                Logger.log(Logger.Level.INFO, "Constructing BM25 index...");
                LuceneFactory.build(new File(Configuration.getKGDir()), true);
                Logger.log(Logger.Level.INFO, "Done constructing BM25 index");
            }

            TimeUnit.SECONDS.sleep(30);
            search = LuceneFactory.get();
            endpoint = Neo4JHandler.getConnector();
            SpringApplication.run(KnowledgeGraph.class, args);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Neo4J or BM25 exception: " + e.getMessage());
        }

        catch (InterruptedException e)
        {
            throw new RuntimeException("Could not wait for Neo4J docker container to start");
        }
    }

    @GetMapping("/ping")
    public ResponseEntity<String> ping()
    {
        Logger.log(Logger.Level.INFO, "PING");
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body("pong");
    }

    /**
     * Returns a sub-KG containing entities and text literals of entity objects
     * Warning: This is a potential endpoint that can crash the service if the graph is very large!
     *          A solution would be to only returns textual objects of given entities, but the entity linker service does
     *          not know all entities.
     * @param headers Request headers
     * @return JSON array of entities and objects that are textual literals, as in the example below:
     *          {
     *              "entities": [
     *                  {
     *                      "entity": "<URI>",
     *                      "objects": ["<OBJ_1>",
     *                                  "<OBJ_2>",
     *                                  ...
     *                                  "<OBJ_N>"
     *                                  ]
     *                  },
     *                  ...
     *              ]
     *          }
     * <p>
     * WARNING:
     *      This endpoint can memory crash the service for large KGs since this loads the sub-graph into memory
     */
    @GetMapping("/sub-kg")
    public synchronized ResponseEntity<String> getSubKG(@RequestHeader Map<String, String> headers)
    {
        Neo4JReader reader = new Neo4JReader(endpoint);
        Map<String, Set<String>> subKG = reader.subGraph();
        JsonArray array = new JsonArray();

        for (Map.Entry<String, Set<String>> entry : subKG.entrySet())
        {
            JsonObject entity = new JsonObject();
            JsonArray entityObjects = new JsonArray();
            entity.addProperty("entity", entry.getKey());

            for (String object : entry.getValue())
            {
                entityObjects.add(object);
            }

            entity.add("objects", entityObjects);
            array.add(entity);
        }

        JsonObject json = new JsonObject();
        json.add("entities", array);

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(json.toString());
    }

    /**
     * Perform keyword search for KG entities using BM25
     * @param body Must contain an entry 'query', which contains a keyword query string
     * @return JSON array with results
     */
    @PostMapping("/search")
    public synchronized ResponseEntity<String> searchEntities(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!body.containsKey("query"))
        {
            return ResponseEntity.badRequest().body("Missing 'query' field in request");
        }

        String queryStr = body.get("query");
        List<String> results = search.find(queryStr);
        JsonArray jsonResults = new JsonArray();
        results.forEach(jsonResults::add);

        JsonObject jsonResponse = new JsonObject();
        jsonResponse.add("results", jsonResults);

        return ResponseEntity.ok(jsonResponse.toString());
    }

    /**
     * Inserts table node linked to all passed entity nodes.
     * This is given in a folder of Turtle files
     * JSON format:
     *  {
     *      "folder": "<FOLDER>"
     *  }
     * <p>
     *  The folder must have two .ttl files: one with table ID to entities and one with table ID to types
     *  Table ID to entities must contain lines of the format <TABLE ID URI> <https://schema.org/mentions> <KG entity IRI>
     *  Table ID to types must contain lines of the format <TABLE ID URI> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Table>
     */
    @PostMapping("/insert-links")
    public synchronized ResponseEntity<String> insertLinks(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        if (!headers.containsKey("content-type") || !headers.get("content-type").equals(MediaType.APPLICATION_JSON_VALUE))
        {
            return ResponseEntity.badRequest().body("Content-Type must be " + MediaType.APPLICATION_JSON);
        }

        else if (!body.containsKey("folder"))
        {
            return ResponseEntity.badRequest().body("Missing 'folder' entry in body that specifies folder of table links");
        }

        try
        {
            Neo4JWriter.insertTableToEntities(body.get("folder"), endpoint);
            return ResponseEntity.ok().build();
        }

        catch (IOException e)
        {
            return ResponseEntity.internalServerError().body("Failed inserting table to entity links: " + e.getMessage());
        }
    }

    /**
     * Returns JSON array of types of given entity
     * Body must contain a single JSON entry with entity URI:
     *      {
     *          "entity": "<URI>"
     *      }
     * @return JSON array of entity types of format {"types": ["<TYPE_1>", "<TYPE_2>", ..., "<TYPE_N>"]}
     */
    @PostMapping("/types")
    public synchronized ResponseEntity<String> entityTypes(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String entry = "entity";

        if (!body.containsKey(entry))
        {
            return ResponseEntity.badRequest().body("Missing entry \"" + entry + "\" to specify entity to find types of");
        }

        try
        {
            Neo4JReader reader = new Neo4JReader(endpoint);
            List<Type> types = reader.entityTypes(new Entity(body.get(entry)));

            if (types == null)
            {
                return ResponseEntity.badRequest().body("An IOException was encountered when querying Neo4J for entity types");
            }

            JsonObject object = new JsonObject();
            JsonArray array = new JsonArray(types.size());
            types.forEach(t -> array.add(t.getType()));
            object.add("types", array);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(object.toString());
        }

        catch (RuntimeException e)
        {
            return ResponseEntity.internalServerError().body("Neo4J error: " + e.getMessage());
        }
    }

    /**
     * Returns JSON array of predicates of a given entity
     * Body must contain a single JSON entry with entity URI
     *      {
     *          "entity": "<URI>"
     *      }
     * @return JSON array pf entity predicates of format {"predicates": ["<PREDICATE_1>", "<PREDICATE_2>", ..., "<PREDICATE_N>"]}
     */
    @PostMapping("/predicates")
    public synchronized ResponseEntity<String> entityPredicates(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String entry = "entity";

        if (!body.containsKey(entry))
        {
            return ResponseEntity.badRequest().body("Missing entry \"" + entry + "\" to specify entity to find predicates of");
        }

        try
        {
            Neo4JReader reader = new Neo4JReader(endpoint);
            List<String> entityPredicates = reader.entityPredicates(new Entity(body.get(entry)));

            if (entityPredicates == null)
            {
                return ResponseEntity.badRequest().body("An IOException was encountered when querying Neo4J for entity predicates");
            }

            JsonObject object = new JsonObject();
            JsonArray array = new JsonArray(entityPredicates.size());
            entityPredicates.forEach(array::add);
            object.add("predicates", array);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(object.toString());
        }

        catch (RuntimeException e)
        {
            return ResponseEntity.internalServerError().body("Neo4J error: " + e.getMessage());
        }
    }

    @GetMapping("/size")
    public synchronized ResponseEntity<String> size(@RequestHeader Map<String, String> headers)
    {
        Neo4JReader reader = new Neo4JReader(endpoint);
        return ResponseEntity.ok(String.valueOf(reader.size()));
    }

    /**
     * Looks for entities that has link to given Wikipedia entity.
     * Body must contain a single JSON entry:
     *      {
     *          "wiki": "<WIKIPEDIA ENTITY>"
     *      }
     * @return KG entity corresponding to Wikipedia URL
     */
    @PostMapping("/from-wiki-link")
    public synchronized ResponseEntity<String> wikiToKG(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String entityKey = "wiki";

        if (!body.containsKey(entityKey))
        {
            return ResponseEntity.badRequest().body("Missing \"" + entityKey + "\" entry in JSON body as input entity");
        }

        String entity = body.get(entityKey);

        if (entity.contains("https"))
        {
            entity = entity.replace("https", "http");
        }

        List<String> kgEntities = endpoint.searchWikiLink(entity);
        return ResponseEntity.ok(kgEntities.isEmpty() ? "None" : kgEntities.get(0));
    }

    /**
     * Check if entity exists in KG or is found as object to a sameAs predicate
     * Body must contain a single JSON entry:
     *      {
     *          "entity": "<ENTITY URI>"
     *      }
     * @return "true" or "false" as string depending on the result
     */
    @PostMapping("/exists")
    public synchronized ResponseEntity<String> exists(@RequestHeader Map<String, String> headers, @RequestBody Map<String, String> body)
    {
        final String entityKey = "entity";

        if (!body.containsKey(entityKey))
        {
            return ResponseEntity.badRequest().body("Missing \"" + entityKey + "\" entry in JSON body as input entity");
        }

        String uri = body.get(entityKey);

        if (uri.contains("https"))
        {
            uri = uri.replace("https", "http");
        }

        if (endpoint.entityExists(uri))
        {
            return ResponseEntity.ok(uri);
        }

        List<String> foundBySameAs = endpoint.searchSameAs(uri);

        if (!foundBySameAs.isEmpty())
        {
            return ResponseEntity.ok(foundBySameAs.get(0));
        }

        return ResponseEntity.badRequest().body("Entity does not exist");
    }
}