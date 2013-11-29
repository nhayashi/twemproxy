/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>

#define KETAMAP_DISPATCH_MAX_POINT 0xffffffffU

rstatus_t
ketamap_update_with_ketama_points(struct server_pool *pool, int64_t now)
{
    uint32_t server_index;        /* server index */
    uint32_t nserver;             /* # server - live and dead */
    uint32_t pointer_counter = 0; /* # pointers on continuum */
    uint32_t continuum_index = 0; /* continuum index */

    nserver = array_n(&pool->server);
    for (server_index = 0; server_index < nserver; server_index++) {
        struct server *server = array_get(&pool->server, server_index);

        if (pool->auto_eject_hosts && server->next_retry > now) {
            continue;
        }

        static const char delim = '\0';
        unsigned int crc32, point;
        int count, i;

        uint8_t *host_port_delim;
        int host_len;

        static const int sep = ':';
        host_port_delim = strrchr(server->name.data, sep);
        if (host_port_delim) {
            host_len = host_port_delim - server->name.data;
        } else {
            host_len = server->name.len;
        }

        char host[host_len];
        memcpy(host, server->name.data, host_len);

        int port_num = server->port;
        uint16_t port_tmp = server->port;
        int port_len = 0;

        do {
            port_tmp /= 10;
            port_len++;
        } while (port_tmp > 0);

        char port_digits[port_len];
        for (i = 0; i < port_len; i++) {
            int n = port_num % 10;
            port_digits[port_len-1 - i] = '0'+n;
            port_num /= 10;
        }

        crc32 = hash_crc32a(host, host_len);
        crc32 = hash_crc32a_add(crc32, &delim, 1);
        crc32 = hash_crc32a_add(crc32, port_digits, port_len);

        point = 0;
        count = pool->ketama_points * server->weight / 100.0 + 0.5;
        pointer_counter += count;

        for (i = 0; i < count; ++i) {
            char point_buf[4];
            uint32_t point_index;

            point_buf[0] = point & 0xff;
            point_buf[1] = (point >> 8) & 0xff;
            point_buf[2] = (point >> 16) & 0xff;
            point_buf[3] = (point >> 24) & 0xff;

            point = hash_crc32a_add(crc32, point_buf, 4);

            if (continuum_index == 0) {
                pool->continuum[continuum_index].index = server_index;
                pool->continuum[continuum_index].value = point;
            } else {
                struct continuum *dispatched_continuum =
                  ketamap_dispatch(pool->continuum, continuum_index, point);
                point_index = dispatched_continuum - pool->continuum;
                if (dispatched_continuum == pool->continuum &&
                  point > dispatched_continuum->value) {
                    pool->continuum[continuum_index].index = server_index;
                    pool->continuum[continuum_index].value = point;
                } else {
                    while (point_index != continuum_index &&
                      point == pool->continuum[point_index].value) {
                        point_index++;
                    }
                    if (point_index != continuum_index) {
                        memmove(pool->continuum + point_index + 1, pool->continuum + point_index, (continuum_index - point_index) * sizeof(struct continuum));
                    }
                    pool->continuum[point_index].index = server_index;
                    pool->continuum[point_index].value = point;
                }
            }
            continuum_index++;
        }
    }
    pool->ncontinuum = pointer_counter;
    return NC_OK;
}

rstatus_t
ketamap_update_without_ketama_points(struct server_pool *pool, int64_t now)
{
    uint32_t server_index;        /* server index */
    uint32_t nserver;             /* # server - live and dead */
    uint32_t pointer_counter = 0; /* # pointers on continuum */
    uint32_t continuum_index = 0; /* continuum index */
    uint32_t total_weight = 0;    /* total live server weight */
    double scale;

    nserver = array_n(&pool->server);
    for (server_index = 0; server_index < nserver; server_index++) {
        struct server *server = array_get(&pool->server, server_index);

        if (pool->auto_eject_hosts && server->next_retry > now) {
            continue;
        }

        pointer_counter++;

        uint32_t weight = server->weight / 100.0 + 0.5;
        total_weight += weight;
        scale = (double) weight / total_weight;

        int i;
        for (i = 0; i < continuum_index; i++)
            pool->continuum[i].value -=
              (double) pool->continuum[i].value * scale;

        pool->continuum[continuum_index].value = KETAMAP_DISPATCH_MAX_POINT;
        pool->continuum[continuum_index].index = server_index;
        continuum_index++;
    }
    pool->ncontinuum = pointer_counter;
    pool->total_weight = total_weight;
    return NC_OK;
}

rstatus_t
ketamap_update(struct server_pool *pool)
{
    uint32_t nserver;      /* # server - live and dead */
    uint32_t nlive_server; /* # live server */
    uint32_t server_index; /* server index */
    uint32_t total_ncontinuum; /* total live server ncontinuum */
    int64_t now;           /* current timestamp in usec */

    ASSERT(array_n(&pool->server) > 0);

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    /*
     * Count live servers and total weight, and also update the next time to
     * rebuild the distribution
     */
    nserver = array_n(&pool->server);
    nlive_server = 0;
    total_ncontinuum = 0;
    pool->next_rebuild = 0LL;
    for (server_index = 0; server_index < nserver; server_index++) {
        struct server *server = array_get(&pool->server, server_index);

        if (pool->auto_eject_hosts) {
            if (server->next_retry <= now) {
                server->next_retry = 0LL;
                nlive_server++;
            } else if (pool->next_rebuild == 0LL ||
                       server->next_retry < pool->next_rebuild) {
                pool->next_rebuild = server->next_retry;
            }
        } else {
            nlive_server++;
        }

        ASSERT(server->weight > 0);

        /* count weight only for live servers */
        if (!pool->auto_eject_hosts || server->next_retry <= now) {
            total_ncontinuum +=
              pool->ketama_points * server->weight / 100.0 + 0.5;
        }
    }

    pool->nlive_server = nlive_server;

    if (nlive_server == 0) {
        log_debug(LOG_DEBUG, "no live servers for pool %"PRIu32" '%.*s'",
                  pool->idx, pool->name.len, pool->name.data);

        return NC_OK;
    }
    log_debug(LOG_DEBUG, "%"PRIu32" of %"PRIu32" servers are live for pool "
              "%"PRIu32" '%.*s'", nlive_server, nserver, pool->idx,
              pool->name.len, pool->name.data);

    /*
     * Allocate the continuum for the pool, the first time, and every time we
     * add a new server to the pool
     */
    if (nlive_server > pool->nserver_continuum) {
        struct continuum *continuum;
        uint32_t nserver_continuum = nlive_server;
        uint32_t ncontinuum;
        if (pool->ketama_points > 0)
            ncontinuum = total_ncontinuum;
        else
            ncontinuum = nserver_continuum;

        continuum =
          nc_realloc(pool->continuum, sizeof(*continuum) * ncontinuum);

        if (continuum == NULL) {
            return NC_ENOMEM;
        }

        pool->continuum = continuum;
        pool->nserver_continuum = nserver_continuum;
        /* pool->ncontinuum is initialized later as it could be <= ncontinuum */
    }

    /*
     * Build a continuum with the servers that are live and points from
     * these servers that are proportial to their weight
     */
    if (pool->ketama_points > 0)
        return ketamap_update_with_ketama_points(pool, now);
    else
        return ketamap_update_without_ketama_points(pool, now);
}

struct continuum *
ketamap_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *begin, *end, *left, *right, *middle;

    ASSERT(continuum != NULL);
    ASSERT(ncontinuum != 0);

    begin = left = continuum;
    end = right = continuum + ncontinuum;

    while (left < right) {
        middle = left + (right - left) / 2;
        if (middle->value < hash) {
          left = middle + 1;
        } else if (middle->value > hash) {
          right = middle;
        } else {
            while (middle != begin && (middle - 1)->value == hash)
              --middle;
            return middle;
        }
    }

    if (right == end) {
        right = begin;
    }

    return right;
}

struct continuum *
ketamap_dispatch0(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash, uint32_t total_weight)
{
    unsigned int crc32 = (hash >> 16) & 0x00007fffU;
    unsigned int point = crc32 % total_weight;
    point = (double) point / total_weight * KETAMAP_DISPATCH_MAX_POINT + 0.5;
    point += 1;
    return ketamap_dispatch(continuum, ncontinuum, point);
}

