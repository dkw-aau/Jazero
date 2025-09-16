#include "driver/jdlc.h"

struct properties init_params_insert_embeddings(void)
{
    return prop_init();
}

struct properties init_params_load()
{
    struct properties props = prop_init();
    prop_insert(&props, "Content-Type", "application/json", 16);
    return props;
}

struct properties init_params_search(void)
{
    return prop_init();
}
