#include "driver/jdlc.h"
#include <stdio.h>

const char *load_embeddings_body(char *buffer, const char *file, const char *delimiter)
{
    sprintf(buffer, "{\"file\": \"%s\", \"delimiter\": \"%s\"}",
            file, delimiter);
    return buffer;
}

const char *load_body(char *buffer, const char *table_dir, const char *table_entity_prefix, const char *kg_prefix, uint8_t progressive)
{
    char *progressive_str = progressive ? "true": "false";
    sprintf(buffer, "{\"directory\": \"%s\", \"table-prefix\": \"%s\", \"kg-prefix\": \"%s\", \"progressive\": \"%s\"}",
            table_dir, table_entity_prefix, kg_prefix, progressive_str);
    return buffer;
}

const char *search_body(char *buffer, uint32_t top_k, enum entity_similarity entity_sim, enum cosine_function function,
                        enum similarity_measure sim, enum prefilter prefilter, int query_time, query q)
{
    const char *cos_func_str = c2str(function), *sim_str = s2str(sim), *prefilter_str = p2str(prefilter),
                                *entity_sim_str = e2str(entity_sim);
    const char *query_str = q2str(q);
    sprintf(buffer, "{\"top-k\": \"%d\", \"entity-similarity\": \"%s\", \"cosine-function\": \"%s\", "
                  "\"single-column-per-query-entity\": \"true\", \"weighted-jaccard\": \"false\", "
                  "\"use-max-similarity-per-column\": \"true\", \"similarity-measure\": \"%s\", \"pre-filter\": \"%s\", "
                  "\"query-time\": %d, \"query\": \"%s\"}",
                  top_k, entity_sim_str, cos_func_str, sim_str, prefilter_str, query_time, query_str);
    return buffer;
}

const char *keyword_search_body(char *buffer, const char *query)
{
    sprintf(buffer, "{\"query\": \"%s\"}", query);
    return buffer;
}

const char *add_user_body(char *buffer, user new_user)
{
    sprintf(buffer, "{\"new-username\": \"%s\", \"new-password\": \"%s\"}", new_user.username, new_user.password);
    return buffer;
}

const char *remove_user_body(char *buffer, const char *username)
{
    sprintf(buffer, "{\"old-username\": \"%s\"}", username);
    return buffer;
}
