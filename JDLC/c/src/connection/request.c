#include <connection/request.h>
#include <curl/curl.h>
#include <string.h>
#include <stdio.h>

struct request make_request(enum op operation, struct properties props, const char *restrict body)
{
    char *copy = (char *) malloc(sizeof(char) * (body == NULL ? 0 : strlen(body)) + 1);

    if (copy != NULL && body != NULL)
    {
        strcpy(copy, body);
    }

    else
    {
        strcpy(copy, "");
    }

    return (struct request) {.operation = operation, .props = props, .body = copy};
}

static char *get_url(struct address addr)
{
    char *url = NULL;
    char *protocol = "";
    int port_length = 6;
    size_t size = sizeof(char) * (strlen(addr.host) + strlen(addr.path) + port_length) + 5;

    if (strstr(addr.host, "http") == NULL)
    {
        size += 7;
        protocol = "http://";
    }

    url = (char *) malloc(size);

    if (url == NULL)
    {
        return NULL;
    }

    sprintf(url, "%s%s:%d%s", protocol, addr.host, addr.port, addr.path);
    return url;
}

static size_t callback(void *contents, size_t size, size_t n, void *buffer)
{
    size_t full_size = size * n;
    struct request_response *res = (struct request_response *) buffer;
    res->length += full_size;
    char *new_buffer = (char *) realloc(res->msg, res->length);

    if (new_buffer == NULL)
    {
        return -1;
    }

    res->msg = new_buffer;
    memcpy(res->msg + (res->length - full_size), contents, full_size);
    res->msg[res->length] = '\0';
    return full_size;
}

static inline void prepare(CURL *handle, const char *url, const struct curl_slist *headers, struct request req, struct request_response *res)
{
    curl_global_init(CURL_GLOBAL_ALL);
    curl_easy_setopt(handle, CURLOPT_URL, url);
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);
    //curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, req.op == PING ? "GET" : "POST");     // This can be problematic for redirects!
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, callback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, res);

    if (req.operation == POST && req.body != NULL)
    {
        curl_easy_setopt(handle, CURLOPT_POSTFIELDS, req.body);
    }
}

static struct curl_slist make_headers(struct properties props)
{
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    uint32_t count = prop_count(props);

    for (uint32_t i = 0; i < count; i++)
    {
        const char *key = prop_key(props, i);
        char *buffer = (char *) calloc(1000, sizeof(char));

        if (key != NULL && buffer != NULL)
        {
            int8_t ret = prop_get(props, key, buffer);
            char *header = (char *) malloc(sizeof(char) * strlen(key) + strlen(buffer) + 5);

            if (ret && headers != NULL)
            {
                sprintf(header, "%s: %s", key, buffer);
                headers = curl_slist_append(headers, header);
                free(header);
            }

            free(buffer);
        }
    }

    return *headers;
}

struct request_response request_perform(struct request req, struct address addr)
{
    CURL *handle = curl_easy_init();
    char *url = get_url(addr);
    struct request_response res = {.length = 0, .msg = (char *) malloc(1), .status = 200};
    struct curl_slist headers = make_headers(req.props);
    prepare(handle, url, &headers, req, &res);

    CURLcode code = curl_easy_perform(handle);

    if (code != CURLE_OK)
    {
        res.status = 400;
        res.msg = "Request failed";
    }

    free(url);
    curl_easy_cleanup(handle);
    curl_global_cleanup();
    return res;
}
