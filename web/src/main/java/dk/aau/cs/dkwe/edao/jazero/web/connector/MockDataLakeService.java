package dk.aau.cs.dkwe.edao.jazero.web.connector;

import dk.aau.cs.dkwe.edao.connector.DataLake;
import dk.aau.cs.dkwe.edao.jazero.communication.Response;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.Service;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.User;
import dk.aau.cs.dkwe.edao.structures.Query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MockDataLakeService extends Service implements DataLake
{
    private final Random rand = new Random();

    public MockDataLakeService()
    {
        super("mock", 0, false);
    }

    @Override
    public Response ping()
    {
        return new Response(200, "pong");
    }

    @Override
    public Result search(Query query, int k, TableSearch.EntitySimilarity entitySimilarity, boolean prefilter)
    {
        return search(query, k, entitySimilarity, 0, prefilter);
    }

    @Override
    public Result search(Query query, int k, TableSearch.EntitySimilarity entitySimilarity, int queryWait, boolean prefilter)
    {
        try (BufferedReader reader = new BufferedReader(new FileReader("first_output.json")))
        {
            int c;
            StringBuilder builder = new StringBuilder();
            Thread.sleep(3000);

            while ((c = reader.read()) != -1)
            {
                builder.append((char) c);
            }

            return Result.fromJson(builder.toString());
        }

        catch (IOException | InterruptedException e)
        {
            return new Result(0, List.of(), -1.0, -1.0, Map.of());
        }
    }

    @Override
    public Response keywordSearch(String keyword)
    {
        List<String> ranking = List.of();
        String query = keyword.toLowerCase();

        if (query.startsWith("m"))
        {
            ranking = List.of("https://dbpedia.org/page/MERS", "https://dbpedia.org/page/Measles", "https://dbpedia.org/page/Malaria", "https://dbpedia.org/page/Middle_East");
        }

        else if (query.startsWith("z"))
        {
            ranking = List.of("https://dbpedia.org/page/Zanamivir");
        }

        else if (query.startsWith("o"))
        {
            ranking = List.of("https://dbpedia.org/page/Oseltamivir");
        }

        else if (query.startsWith("r"))
        {
            ranking = List.of("https://dbpedia.org/page/Rhinovirus", "https://dbpedia.org/page/Rabies", "https://dbpedia.org/page/Respiratory_system", "https://dbpedia.org/page/Respiratory_syncytial_virus");
        }

        else if (query.startsWith("b"))
        {
            ranking = List.of("https://dbpedia.org/page/Baloxavir", "https://dbpedia.org/page/Bird", "https://dbpedia.org/page/Bat", "https://dbpedia.org/page/Bronchitis");
        }

        else if (query.startsWith("c"))
        {
            ranking = List.of("https://dbpedia.org/page/Chimpanzee", "https://dbpedia.org/page/Cancer", "https://dbpedia.org/page/Cholera");
        }

        else if (query.startsWith("i"))
        {
            ranking = List.of("https://dbpedia.org/page/Influenza", "https://dbpedia.org/page/Italy", "https://dbpedia.org/page/India");
        }

        else if (query.startsWith("sa"))
        {
            ranking = List.of("https://dbpedia.org/page/SARS");
        }

        else if (query.startsWith("sw"))
        {
            ranking = List.of("https://dbpedia.org/page/Switzerland");
        }

        else if (query.startsWith("un"))
        {
            ranking = List.of("https://dbpedia.org/page/University_of_Basel");
        }

        else if (query.startsWith("u"))
        {
            ranking = List.of("https://dbpedia.org/page/USA");
        }

        else if (query.startsWith("ad"))
        {
            ranking = List.of("https://dbpedia.org/page/Adenovirus");
        }

        else if (query.startsWith("a"))
        {
            ranking = List.of("https://dbpedia.org/page/AOU");
        }

        else if (query.startsWith("p"))
        {
            ranking = List.of("https://dbpedia.org/page/Peramivir");
        }

        else if (query.startsWith("g"))
        {
            ranking = List.of("https://dbpedia.org/page/GlaxoSmithKline");
        }

        return new Response(200, ranking);
    }

    @Override
    public Response clear()
    {
        return new Response(200, "");
    }

    @Override
    public Response clearEmbeddings()
    {
        return new Response(200, "");
    }

    @Override
    public Response removeTable(String tableId)
    {
        return new Response(200, "");
    }

    @Override
    public Response addUser(User newUser)
    {
        return new Response(200, "");
    }

    @Override
    public Response removeUser(User removedUser)
    {
        return new Response(200, "");
    }

    @Override
    public Response count(String uri)
    {
        int randomCount = this.rand.nextInt(1000);
        return new Response(200, String.valueOf(randomCount));
    }

    @Override
    public Response stats()
    {
        int entities = Math.abs(this.rand.nextInt()) % 10000000;
        String stats = "{\"entities\": " + entities + ", \"types\": " + (int) Math.ceil(entities * 3.8) +
                ", \"predicates\": " + (int) Math.ceil(entities * 5.5) + ", \"embeddings\": " + (int) Math.ceil(entities * 0.8) +
                ", \"linked cells\": " + entities + ", \"tables\": " + (int) Math.ceil((double) entities / 175) + "}";

        return new Response(200, stats);
    }

    @Override
    public Response tableStats(String filename)
    {
        int entities = (int) Math.ceil(122 * 5 * 0.277);
        String stats = "{\"entities\": " + entities + "\", \"Types\": " + (int) Math.ceil(entities * 3.8) +
                ", \"Predicates\": " + (int) Math.ceil(entities * 5.5) + ", \"Embeddings\": " + (int) Math.ceil(entities * 0.8) +
                ", \"Table rows\": 122, \"Table columns\": 5}";

        return new Response(200, stats);
    }
}
