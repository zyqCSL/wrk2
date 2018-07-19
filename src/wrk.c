// Copyright (C) 2012 - Will Glozer.  All rights reserved.

#include "wrk.h"
#include "script.h"
#include "main.h"
#include "hdr_histogram.h"
#include "stats.h"
#include "assert.h"

// Max recordable latency of 1 day
#define MAX_LATENCY 24L * 60 * 60 * 1000000

uint64_t raw_latency[MAXTHREADS][MAXL];
pthread_mutex_t pt_lock;
FILE *pt;

static struct config {
    uint64_t threads;
    uint64_t connections;
    int dist; //0: fixed; 1: exp; 2: normal; 3: zipf
    uint64_t duration;
    uint64_t timeout;
    uint64_t pipeline;
    uint64_t rate;
    uint64_t delay_ms;
    bool     latency;
    bool     dynamic;
    bool     record_all_responses;
    bool     print_all_responses;
    bool     print_realtime_latency;
    char    *script;
    SSL_CTX *ctx;

    // Yanqi
    bool load_by_script;
    uint64_t stats_report_us;
} cfg;

static struct {
    stats *requests;
    pthread_mutex_t mutex;
} statistics;

// Yanqi, periodic stats reports
static struct {
    pthread_mutex_t mutex;
    int threads;
    double percent_resolution;
    int slot;
    int ready_ctr;
    bool* thread_ready;
    uint64_t** thread_latency_stats;
    uint64_t** thread_nginx_stats;
    uint64_t** thread_memc_get_stats;
    uint64_t** thread_memc_find_stats;
    uint64_t** thread_memc_set_stats;
    uint64_t** thread_pure_nginx_stats;
} pstats;

static struct sock sock = {
    .connect  = sock_connect,
    .close    = sock_close,
    .read     = sock_read,
    .write    = sock_write,
    .readable = sock_readable
};

static struct http_parser_settings parser_settings = {
    .on_message_complete = response_complete
};

static volatile sig_atomic_t stop = 0;

static void handler(int sig) {
    stop = 1;
}

static void usage() {
    printf("Usage: wrk <options> <url>                                           \n"
           "  Options:                                                           \n"
           "    -c, --connections <N>  Connections to keep open                  \n"
           "    -D, --dist             fixed, exp, norm, zipf                    \n"
           "    -P                     Print each request's latency              \n"
           "    -p                     Print 99th latency every #i to file       \n"
           "    -i, --interval         99th tail report interval in second       \n"
           "    -d, --duration    <T>  Duration of test                          \n"
           "    -t, --threads     <N>  Number of threads to use                  \n"
           "                                                                     \n"
           "    -s, --script      <S>  Load Lua script file                      \n"
           "    -H, --header      <H>  Add header to request                     \n"
           "    -L  --latency          Print latency statistics                  \n"
           "    -U  --timeout     <T>  Socket/request timeout                    \n"
           "    -B, --batch_latency    Measure latency of whole                  \n"
           "                           batches of pipelined ops                  \n"
           "                           (as opposed to each op)                   \n"
           "    -v, --version          Print version details                     \n"
           "    -R, --rate        <T>  work rate (throughput)                    \n"
           "                           in requests/sec (total)                   \n"
           "                           [Required Parameter]                      \n"
           "                                                                     \n"
           "                                                                     \n"
           "  Numeric arguments may include a SI unit (1k, 1M, 1G)               \n"
           "  Time arguments may include a time unit (2s, 2m, 2h)                \n");
}

int main(int argc, char **argv) {
    char *url, **headers = zmalloc(argc * sizeof(char *));
    struct http_parser_url parts = {};

    if (parse_args(&cfg, &url, &parts, headers, argc, argv)) {
        usage();
        exit(1);
    }

    char *schema  = copy_url_part(url, &parts, UF_SCHEMA);
    char *host    = copy_url_part(url, &parts, UF_HOST);
    char *port    = copy_url_part(url, &parts, UF_PORT);
    char *service = port ? port : schema;

    if (!strncmp("https", schema, 5)) {
        if ((cfg.ctx = ssl_init()) == NULL) {
            fprintf(stderr, "unable to initialize SSL\n");
            ERR_print_errors_fp(stderr);
            exit(1);
        }
        sock.connect  = ssl_connect;
        sock.close    = ssl_close;
        sock.read     = ssl_read;
        sock.write    = ssl_write;
        sock.readable = ssl_readable;
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  SIG_IGN);

    pt = fopen("/filer-01/yz2297/3tier_sched/pt.txt", "w");
    assert (pthread_mutex_init(&pt_lock, NULL) == 0);

    pthread_mutex_init(&statistics.mutex, NULL);
    statistics.requests = stats_alloc(10);
    // Yanqi, periodic report
    if(cfg.print_realtime_latency)
        init_pstats(cfg.threads);

    thread *threads = zcalloc(cfg.threads * sizeof(thread));

    hdr_init(1, MAX_LATENCY, 3, &(statistics.requests->histogram));

    lua_State *L = script_create(cfg.script, url, headers);

    // Yanqi, read loads from script
    int load_arr_len = 0;
    uint64_t* load_intervals = NULL;
    uint64_t* load_rates = NULL;
    if(cfg.load_by_script) {
        if(script_get_load(L, &load_arr_len, &load_intervals, &load_rates))
            return -1;
        cfg.duration = 0;
        for(int i = 0; i < load_arr_len; ++i) {
            cfg.duration += load_intervals[i];
        }
    }

    if (!script_resolve(L, host, service)) {
        char *msg = strerror(errno);
        fprintf(stderr, "unable to connect to %s:%s %s\n", host, service, msg);
        exit(1);
    }

    uint64_t connections = cfg.connections / cfg.threads;
    unsigned throughput = 0;
    if(!cfg.load_by_script)
        throughput = cfg.rate / cfg.threads;
    else
        throughput = load_rates[0] / cfg.threads;
    uint64_t stop_at     = time_us() + (cfg.duration * 1000000);

    uint64_t base_start_time = time_us();
    for (uint64_t i = 0; i < cfg.threads; i++) {
        thread *t = &threads[i];
        t->tid           = i;
        t->loop          = aeCreateEventLoop(10 + cfg.connections * 3);
        t->connections   = connections;
        t->throughput    = throughput;
        t->stop_at       = stop_at;
        t->complete      = 0;
        t->monitored     = 0;
        t->target        = throughput/10; //Shuang
        t->accum_latency = 0;
        t->memcached_misses   = 0;
        t->memcached_hits   = 0;
        t->monitor_event_cnt = 0;
        t->base_start_time = base_start_time;    // Yanqi
        t->throughput_arr_ptr = 0;
        t->throughput_arr = NULL;
        t->throughput_change_us = NULL;

        // verify load change
        t->last_stats_time = base_start_time;
        t->resp_received = 0;
        // periodic stats
        t->stats_report_us = cfg.stats_report_us;
        t->print_realtime_latency = cfg.print_realtime_latency;

        // Yanqi, read load from script
        if(cfg.load_by_script) {
            t->throughput_arr_len = load_arr_len;
            t->throughput_change_us = malloc(sizeof(uint64_t) * load_arr_len);
            for(int x = 0; x < load_arr_len; ++x) {
                uint64_t duration = 0;
                for(int j = 0; j < x; ++j)
                    duration += load_intervals[j];
                t->throughput_change_us[x] = duration * 1000000;
            }

            t->throughput_arr = malloc(sizeof(double) * load_arr_len);
            for(int x = 0; x < load_arr_len; ++x)
                t->throughput_arr[x] = (double)load_rates[x]/cfg.threads;

            // // debug
            // printf("\n");
            // for(int x = 0; x < load_arr_len; ++x) {
            //     printf("throughput: %.2f\n", t->throughput_arr[x]*cfg.threads);
            //     printf("throughput_change_us: %lu\n", (t->throughput_change_us[x])/1000000);
            // }
            // printf("\n");
        }

        t->L = script_create(cfg.script, url, headers);
        script_init(L, t, argc - optind, &argv[optind]);

        if (i == 0) {
            cfg.pipeline = script_verify_request(t->L);
            cfg.dynamic = !script_is_static(t->L);
            if (script_want_response(t->L)) {
                parser_settings.on_header_field = header_field;
                parser_settings.on_header_value = header_value;
                parser_settings.on_body         = response_body;
            }
        }

        if (!t->loop || pthread_create(&t->thread, NULL, &thread_main, t)) {
            char *msg = strerror(errno);
            fprintf(stderr, "unable to create thread %"PRIu64": %s\n", i, msg);
            exit(2);
        }
    }

    struct sigaction sa = {
        .sa_handler = handler,
        .sa_flags   = 0,
    };
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);

    char *time = format_time_s(cfg.duration);
    printf("duration = %lus\n", cfg.duration);
    printf("Running %s test @ %s\n", time, url);
    printf("  %"PRIu64" threads and %"PRIu64" connections\n",
            cfg.threads, cfg.connections);

    uint64_t start    = time_us();
    uint64_t complete = 0;
    uint64_t bytes    = 0;
    errors errors     = { 0 };

    struct hdr_histogram* latency_histogram;
    struct hdr_histogram* real_latency_histogram;

    struct hdr_histogram* nginx_lua_histogram;
    struct hdr_histogram* get_histogram;
    struct hdr_histogram* find_histogram;
    struct hdr_histogram* set_histogram;
    struct hdr_histogram* pure_nginx_histogram;

    struct hdr_histogram* real_nginx_lua_histogram;
    struct hdr_histogram* real_get_histogram;
    struct hdr_histogram* real_find_histogram;
    struct hdr_histogram* real_set_histogram;
    
    hdr_init(1, MAX_LATENCY, 3, &latency_histogram);
    hdr_init(1, MAX_LATENCY, 3, &real_latency_histogram);

    hdr_init(1, MAX_LATENCY, 3, &nginx_lua_histogram);
    hdr_init(1, MAX_LATENCY, 3, &get_histogram);
    hdr_init(1, MAX_LATENCY, 3, &find_histogram);
    hdr_init(1, MAX_LATENCY, 3, &set_histogram);
    hdr_init(1, MAX_LATENCY, 3, &pure_nginx_histogram);

    hdr_init(1, MAX_LATENCY, 3, &real_nginx_lua_histogram);
    hdr_init(1, MAX_LATENCY, 3, &real_get_histogram);
    hdr_init(1, MAX_LATENCY, 3, &real_find_histogram);
    hdr_init(1, MAX_LATENCY, 3, &real_set_histogram);

   

    for (uint64_t i = 0; i < cfg.threads; i++) {
        thread *t = &threads[i];
        pthread_join(t->thread, NULL);
    }

    uint64_t runtime_us = time_us() - start;

    int misses = 0;
    int hits = 0;

    for (uint64_t i = 0; i < cfg.threads; i++) {
        thread *t = &threads[i];
        complete += t->complete;
        bytes    += t->bytes;

        errors.connect += t->errors.connect;
        errors.read    += t->errors.read;
        errors.write   += t->errors.write;
        errors.timeout += t->errors.timeout;
        errors.status  += t->errors.status;

        hdr_add(latency_histogram, t->latency_histogram);
        hdr_add(real_latency_histogram, t->real_latency_histogram);

        hdr_add(nginx_lua_histogram, t->nginx_lua_histogram);
        hdr_add(get_histogram, t->get_histogram);
        hdr_add(find_histogram, t->find_histogram);
        hdr_add(set_histogram, t->set_histogram);
        hdr_add(pure_nginx_histogram, t->pure_nginx_histogram);

        hdr_add(real_nginx_lua_histogram, t->real_nginx_lua_histogram);
        hdr_add(real_get_histogram, t->real_get_histogram);
        hdr_add(real_find_histogram, t->real_find_histogram);
        hdr_add(real_set_histogram, t->real_set_histogram);

        
        if (cfg.print_all_responses) {
            char filename[10] = {0};
            sprintf(filename, "%" PRIu64 ".txt", i);
            FILE* ff = fopen(filename, "w");
            uint64_t nnum=MAXL;
            if ((t->complete) < nnum) nnum = t->complete;
            for (uint64_t j=1; j < nnum; ++j)
                fprintf(ff, "%" PRIu64 "\n", raw_latency[i][j]);
            fclose(ff);
        }
        misses += t->memcached_misses;
        hits += t->memcached_hits;
    }
    fclose(pt);

    long double runtime_s   = runtime_us / 1000000.0;
    long double req_per_s   = complete   / runtime_s;
    long double bytes_per_s = bytes      / runtime_s;

    stats *latency_stats = stats_alloc(10);
    latency_stats->min = hdr_min(latency_histogram);
    latency_stats->max = hdr_max(latency_histogram);
    latency_stats->histogram = latency_histogram;

    print_stats_header();
    print_stats("Latency", latency_stats, format_time_us);
    print_stats("Req/Sec", statistics.requests, format_metric);

    if (cfg.latency) {
        print_hdr_latency(latency_histogram,
                "Total Latency");
        printf("----------------------------------------------------------\n");

        print_hdr_latency(nginx_lua_histogram,
                "Nginx Latency");
        printf("----------------------------------------------------------\n");

        print_hdr_latency(get_histogram,
                "Memcached Get Latency");
        printf("----------------------------------------------------------\n");

        print_hdr_latency(set_histogram,
                "Memcached Set Latency");
        printf("----------------------------------------------------------\n");

        print_hdr_latency(find_histogram,
                "MongoDB Latency");
        printf("----------------------------------------------------------\n");

        print_hdr_latency(pure_nginx_histogram,
                "Pure Nginx Latency");
        printf("----------------------------------------------------------\n");

        printf("Memcached Miss Rate = %3f\n", (double) misses/(misses+hits));

    }

    char *runtime_msg = format_time_us(runtime_us);

    printf("  %"PRIu64" requests in %s, %sB read\n",
            complete, runtime_msg, format_binary(bytes));
    if (errors.connect || errors.read || errors.write || errors.timeout) {
        printf("  Socket errors: connect %d, read %d, write %d, timeout %d\n",
               errors.connect, errors.read, errors.write, errors.timeout);
    }

    if (errors.status) {
        printf("  Non-2xx or 3xx responses: %d\n", errors.status);
    }

    printf("Requests/sec: %9.2Lf\n", req_per_s);
    printf("Transfer/sec: %10sB\n", format_binary(bytes_per_s));

    if (script_has_done(L)) {
        script_summary(L, runtime_us, complete, bytes);
        script_errors(L, &errors);
        script_done(L, latency_stats, statistics.requests);
    }

    return 0;
}

void *thread_main(void *arg) {
    thread *thread = arg;
    aeEventLoop *loop = thread->loop;

    thread->cs = zcalloc(thread->connections * sizeof(connection));
    tinymt64_init(&thread->rand, time_us());
    hdr_init(1, MAX_LATENCY, 3, &thread->latency_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->real_latency_histogram);

    hdr_init(1, MAX_LATENCY, 3, &thread->nginx_lua_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->get_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->find_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->set_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->pure_nginx_histogram);

    hdr_init(1, MAX_LATENCY, 3, &thread->real_nginx_lua_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->real_get_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->real_find_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->real_set_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->real_pure_nginx_histogram);
    hdr_init(1, MAX_LATENCY, 3, &thread->real_nginx_proc_histogram);

    char *request = NULL;
    size_t length = 0;

    if (!cfg.dynamic) {
        script_request(thread->L, &request, &length);
    }
    
    // thread->ff = NULL;
    // if ((cfg.print_realtime_latency) && (thread->tid == 0)) {
    //     char filename[50];
    //     snprintf(filename, 50, "/filer01/yg397/three_tier/%" PRIu64 ".txt", thread->tid);
    //     thread->ff = fopen(filename, "w");
    // }

    char filename[50];
    snprintf(filename, 50, "/filer-01/yz2297/3tier_sched/%" PRIu64 ".txt", thread->tid);
    thread->ff = fopen(filename, "w");


    double throughput = (thread->throughput / 1000000.0) / thread->connections;

    connection *c = thread->cs;

    for (uint64_t i = 0; i < thread->connections; i++, c++) {
        c->thread     = thread;
        c->ssl        = cfg.ctx ? SSL_new(cfg.ctx) : NULL;
        c->request    = request;
        c->length     = length;
        c->interval   = 1000000*thread->connections/thread->throughput;
        c->throughput = throughput;
        c->complete   = 0;
        c->estimate   = 0;
        c->sent       = 0;
        // Stagger connects 1 msec apart within thread:
        aeCreateTimeEvent(loop, i, delayed_initial_connect, c, NULL);
    }

    uint64_t calibrate_delay = CALIBRATE_DELAY_MS + (thread->connections);
    uint64_t timeout_delay = TIMEOUT_INTERVAL_MS + (thread->connections);

    aeCreateTimeEvent(loop, calibrate_delay, calibrate, thread, NULL);
    aeCreateTimeEvent(loop, timeout_delay, check_timeouts, thread, NULL);

    // Yanqi
    // register load change events
    for(int i = 1; i < thread->throughput_arr_len; ++i)
        aeCreateTimeEvent(loop, thread->throughput_change_us[i]/1000, change_throughput, thread, NULL);
    // register stats dump event
    if(thread->print_realtime_latency)
        aeCreateTimeEvent(loop, thread->stats_report_us/1000 + thread->tid, report_stats, thread, NULL);

    thread->start = time_us();
    aeMain(loop);

    aeDeleteEventLoop(loop);
    zfree(thread->cs);
    // if (cfg.print_realtime_latency && thread->tid == 0) fclose(thread->ff);
    if (cfg.print_realtime_latency) fclose(thread->ff);

    return NULL;
}

// Yanqi, periodic report
static void init_pstats(int threads) {
    pthread_mutex_init(&pstats.mutex, NULL);

    pstats.threads = threads;
    pstats.percent_resolution = 1;  // by default, print data at evey 1% granularity
    pstats.slot = (int)(threads * 1.0/pstats.percent_resolution);
    pstats.ready_ctr = 0;
    pstats.thread_ready = malloc(sizeof(bool) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_ready[i] = false;

    pstats.thread_latency_stats = malloc(sizeof(uint64_t*) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_latency_stats[i] = malloc(sizeof(uint64_t) * pstats.slot);

    pstats.thread_nginx_stats = malloc(sizeof(uint64_t*) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_nginx_stats[i] = malloc(sizeof(uint64_t) * pstats.slot);

    pstats.thread_memc_get_stats = malloc(sizeof(uint64_t*) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_memc_get_stats[i] = malloc(sizeof(uint64_t) * pstats.slot);

    pstats.thread_memc_find_stats = malloc(sizeof(uint64_t*) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_memc_find_stats[i] = malloc(sizeof(uint64_t) * pstats.slot);

    pstats.thread_memc_set_stats = malloc(sizeof(uint64_t*) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_memc_set_stats[i] = malloc(sizeof(uint64_t) * pstats.slot);

    pstats.thread_pure_nginx_stats = malloc(sizeof(uint64_t*) * threads);
    for(int i = 0; i < threads; ++i)
        pstats.thread_pure_nginx_stats[i] = malloc(sizeof(uint64_t) * pstats.slot);
}

static int connect_socket(thread *thread, connection *c) {
    struct addrinfo *addr = thread->addr;
    struct aeEventLoop *loop = thread->loop;
    int fd, flags;

    fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);

    flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    if (connect(fd, addr->ai_addr, addr->ai_addrlen) == -1) {
        if (errno != EINPROGRESS) goto error;
    }

    flags = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));

    flags = AE_READABLE | AE_WRITABLE;
    if (aeCreateFileEvent(loop, fd, flags, socket_connected, c) == AE_OK) {
        c->parser.data = c;
        c->fd = fd;
        return fd;
    }

  error:
    thread->errors.connect++;
    close(fd);
    return -1;
}

static int reconnect_socket(thread *thread, connection *c) {
    aeDeleteFileEvent(thread->loop, c->fd, AE_WRITABLE | AE_READABLE);
    sock.close(c);
    close(c->fd);
    return connect_socket(thread, c);
}

static int delayed_initial_connect(aeEventLoop *loop, long long id, void *data) {
    connection* c = data;
    c->thread_start = time_us();
    c->thread_next  = c->thread_start;
    connect_socket(c->thread, c);
    return AE_NOMORE;
}

static int change_throughput(aeEventLoop *loop, long long id, void *data) {
    thread* thread = data;
    thread->throughput_arr_ptr ++;
    assert(thread->throughput_arr_ptr < thread->throughput_arr_len);
    thread->throughput = thread->throughput_arr[thread->throughput_arr_ptr];
    thread->target = (uint64_t)(thread->throughput/10); // make sure still printing every 0.1s

    double throughput = (thread->throughput / 1000000.0) / thread->connections;
    connection *c = thread->cs;
    for (uint64_t i = 0; i < thread->connections; i++, c++) {
        c->interval   = 1000000*thread->connections/thread->throughput;
        c->throughput = throughput;
    }

    // // debug
    // printf("Throughput changed to %.2f at time %lus\n", thread->throughput, (time_us() - thread->base_start_time)/1000000L );

    return AE_NOMORE;
}
static int report_stats(aeEventLoop *loop, long long id, void *data) {
    thread* thread = data;
    // return (int)(thread->stats_report_us/1000);
    
    bool update = false;
    pthread_mutex_lock(&pstats.mutex);
    update = !pstats.thread_ready[thread->tid];
    pthread_mutex_unlock(&pstats.mutex);

    update &= (thread->monitor_event_cnt > 0);

    double start_percent = 100 - pstats.threads;
    if(update) {
        thread->monitor_event_cnt = 0;
        double percent = 0;
        for(int i = 0; i < pstats.slot; ++i) {
            percent = start_percent + i * pstats.percent_resolution;
            pstats.thread_latency_stats[thread->tid][i] =  hdr_value_at_percentile(thread->real_latency_histogram, percent);
            pstats.thread_nginx_stats[thread->tid][i] =  hdr_value_at_percentile(thread->real_nginx_lua_histogram, percent);
            pstats.thread_memc_get_stats[thread->tid][i] =  hdr_value_at_percentile(thread->real_get_histogram, percent);
            pstats.thread_memc_find_stats[thread->tid][i] =  hdr_value_at_percentile(thread->real_find_histogram, percent);
            pstats.thread_memc_set_stats[thread->tid][i] =  hdr_value_at_percentile(thread->real_set_histogram, percent);

            pstats.thread_pure_nginx_stats[thread->tid][i] =  hdr_value_at_percentile(thread->real_pure_nginx_histogram, percent);
        }

        hdr_reset(thread->real_latency_histogram);
        hdr_reset(thread->real_nginx_lua_histogram);
        hdr_reset(thread->real_get_histogram);
        hdr_reset(thread->real_find_histogram);
        hdr_reset(thread->real_pure_nginx_histogram);
        hdr_reset(thread->real_nginx_proc_histogram);
        hdr_reset(thread->real_pure_nginx_histogram);

        pthread_mutex_lock(&pstats.mutex);
        pstats.ready_ctr += 1;
        pstats.thread_ready[thread->tid] = true;
        
        if(pstats.ready_ctr == pstats.threads) {
            pstats.ready_ctr = 0;
            for(int i = 0; i < pstats.threads; ++i)
                pstats.thread_ready[i] = false;

            double end_percent = 100 - percent;
            // print stats
            uint64_t latency = get_99_tail(pstats.thread_latency_stats, pstats.threads, pstats.slot, pstats.percent_resolution, end_percent);
            uint64_t nginx = get_99_tail(pstats.thread_nginx_stats, pstats.threads, pstats.slot, pstats.percent_resolution, end_percent);
            uint64_t memc_get = get_99_tail(pstats.thread_memc_get_stats, pstats.threads, pstats.slot, pstats.percent_resolution, end_percent);
            uint64_t memc_find = get_99_tail(pstats.thread_memc_find_stats, pstats.threads, pstats.slot, pstats.percent_resolution, end_percent);
            uint64_t memc_set = get_99_tail(pstats.thread_memc_set_stats, pstats.threads, pstats.slot, pstats.percent_resolution, end_percent);
            uint64_t pure_nginx = get_99_tail(pstats.thread_pure_nginx_stats, pstats.threads, pstats.slot, pstats.percent_resolution, end_percent);
    
            pthread_mutex_unlock(&pstats.mutex);

            // dump stats to file
            pthread_mutex_lock(&pt_lock);
            // printf("write to pt\n");    
            fprintf(pt, "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\n", latency, nginx, memc_get, memc_set, memc_find, pure_nginx);
            fflush(pt); 
            pthread_mutex_unlock(&pt_lock); 
        } else
            pthread_mutex_unlock(&pstats.mutex);
    }

    

    return (int)(thread->stats_report_us/1000);
}

static uint64_t get_99_tail(uint64_t** stats, int threads, int slot, double resolution, double end_percent) {
    double cum_percent = 100;
    uint64_t cur_val = 0;
    uint64_t prev_val = 0;
    int* ptr = zmalloc(sizeof(int) * threads);
    for(int i = 0; i < threads; ++i)
        ptr[i] = slot - 1;
    while(cum_percent > 99.0) {
        // printf("cum_percent = %f\n", cum_percent);
        int max_thread = -1;
        uint64_t max_val = 0;
        for(int i = 0; i < threads; ++i) {
            if(ptr[i] < 0)
                continue;
            if(max_thread < 0 || max_val < stats[i][ptr[i]]) {
                max_thread = i;
                max_val = stats[i][ptr[i]];
            }
        }

        // printf("max_val = %lu\n", max_val);

        assert(max_thread >= 0);
        if(cur_val != 0)
            assert(max_val <= cur_val);
        prev_val = cur_val;
        cur_val = max_val;
        if(ptr[max_thread] == slot - 1)
            cum_percent -= 1.0/threads * end_percent;
        else
            cum_percent -= 1.0/threads * resolution;
        // printf("cum_percent after = %f\n", cum_percent);
        ptr[max_thread] -= 1;
    }
    zfree(ptr);

    // printf("prev_val = %lu\n\n", prev_val);
    return prev_val;
}

static int calibrate(aeEventLoop *loop, long long id, void *data) {
    thread *thread = data;

    long double mean = hdr_mean(thread->latency_histogram);
    long double latency = hdr_value_at_percentile(
            thread->latency_histogram, 90.0) / 1000.0L;
    long double interval = MAX(latency * 2, 10);

    if (mean == 0) return CALIBRATE_DELAY_MS;

    thread->mean     = (uint64_t) mean;
    hdr_reset(thread->latency_histogram);

    thread->start    = time_us();
    thread->interval = interval;
    thread->requests = 0;

    printf("  Thread calibration: mean lat.: %.3fms, rate sampling interval: %dms\n",
            (thread->mean)/1000.0,
            thread->interval);

    aeCreateTimeEvent(loop, thread->interval, sample_rate, thread, NULL);

    return AE_NOMORE;
}

static int check_timeouts(aeEventLoop *loop, long long id, void *data) {
    thread *thread = data;
    connection *c  = thread->cs;
    uint64_t now   = time_us();

    uint64_t maxAge = now - (cfg.timeout * 1000);

    for (uint64_t i = 0; i < thread->connections; i++, c++) {
        if (maxAge > c->start) {
            thread->errors.timeout++;
        }
    }

    if (stop || now >= thread->stop_at) {
        aeStop(loop);
    }

    return TIMEOUT_INTERVAL_MS;
}

static int sample_rate(aeEventLoop *loop, long long id, void *data) {
    thread *thread = data;

    uint64_t elapsed_ms = (time_us() - thread->start) / 1000;
    uint64_t requests = (thread->requests / (double) elapsed_ms) * 1000;

    pthread_mutex_lock(&statistics.mutex);
    stats_record(statistics.requests, requests);
    pthread_mutex_unlock(&statistics.mutex);

    thread->requests = 0;
    thread->start    = time_us();

    return thread->interval;
}

static int header_field(http_parser *parser, const char *at, size_t len) {
    connection *c = parser->data;
    if (c->state == VALUE) {
        *c->headers.cursor++ = '\0';
        c->state = FIELD;
    }
    // printf("print %s\n",at);
    buffer_append(&c->headers, at, len);
    return 0;
}

static int header_value(http_parser *parser, const char *at, size_t len) {
    connection *c = parser->data;
    if (c->state == FIELD) {
        *c->headers.cursor++ = '\0';
        c->state = VALUE;
    }
    
    buffer_append(&c->headers, at, len);
    return 0;
}

static int response_body(http_parser *parser, const char *at, size_t len) {
    connection *c = parser->data;
    buffer_append(&c->body, at, len);
    return 0;
}

uint64_t gen_zipf(connection *conn)
{
    static int first = 1;      // Static first time flag
    static double c = 0;          // Normalization constant
    static double scalar = 0;
    double z;                     // Uniform random number (0 < z < 1)
    double sum_prob;              // Sum of probabilities
    double zipf_value;            // Computed exponential value to be returned
    int n = 100;
    double alpha = 3;

    // Compute normalization constant on first call only
    if (first == 1) {
        for (int i=1; i<=n; i++)
            c = c + (1.0 / pow((double) i, alpha));
        c = 1.0 / c;
        for (int i=1; i<=n; i++) {
            double prob = c / pow((double) i, alpha);
            scalar = scalar + i*prob;
        }
        scalar = conn->interval / scalar;
        first = 0;
    }

  // Pull a uniform random number (0 < z < 1)
    do {
        z = (double)rand()/RAND_MAX;
    } while ((z == 0) || (z == 1));

    // Map z to the value
    sum_prob = 0;
    for (int i=1; i<=n; i++) {
        sum_prob = sum_prob + c / pow((double) i, alpha);
        if (sum_prob >= z) {
            zipf_value = i;
            break;
        }
    }
    return (uint64_t)(zipf_value*scalar);
}

uint64_t gen_exp(connection *c) {
    double z;
    double exp_value;
    do {
        z = (double)rand()/RAND_MAX;
    } while ((z == 0) || (z == 1));
    exp_value = (-log(z)*(c->interval));
    //printf("%.2f %"PRIu64"\n", exp_value, (uint64_t)(exp_value));
    return (uint64_t)(exp_value);
}

uint64_t gen_next(connection *c) {
    if (cfg.dist == 0) { // FIXED
        return c->interval;
    }
    else if (cfg.dist == 1) { // EXP
        return gen_exp(c);
    }
    else if (cfg.dist == 2) {
    }
    else if (cfg.dist == 3) {
       return gen_zipf(c);
    }
    return 0;
}

static uint64_t usec_to_next_send(connection *c) {
    uint64_t now = time_us();
    //c->thread_next = c->thread_start + c->sent/c->throughput;
    //printf("%f\n", 1/c->throughput);
    if (c->estimate <= c->sent) {
        ++c->estimate;
        c->thread_next += gen_next(c);
    }
    if ((c->thread_next) > now) 
        return c->thread_next - now;
    else
        return 0;
}

static int delay_request(aeEventLoop *loop, long long id, void *data) {
    connection* c = data;
    uint64_t time_usec_to_wait = usec_to_next_send(c);
    if (time_usec_to_wait) {
        return round((time_usec_to_wait / 1000.0L) + 0.5); /* don't send, wait */
    }
    aeCreateFileEvent(c->thread->loop, c->fd, AE_WRITABLE, socket_writeable, c);
    return AE_NOMORE;
}

static int response_complete(http_parser *parser) {
    connection *c = parser->data;
    thread *thread = c->thread;
    uint64_t now = time_us();
    int status = parser->status_code;

    thread->complete++;
    // printf("complete %"PRIu64"\n", thread->complete);
    thread->requests++;

    thread->monitor_event_cnt++;

    if (status > 399) {
        thread->errors.status++;
    }

   
    struct latencies l; 
    if (c->headers.buffer) {
        *c->headers.cursor++ = '\0';
        
        
        script_response_get_latency(thread->L, status, &c->headers, &c->body, &l);
        
        
        c->state = FIELD;
    }
        
   

    if (now >= thread->stop_at) {
        aeStop(thread->loop);
        goto done;
    }

    // /****
    // * Yanqi, verify load change
    // **/
    // thread->resp_received += 1;
    // if(now - thread->last_stats_time > 1000000) {
    //     printf("resp_recv = %lu\n", thread->resp_received);
    //     printf("thread throughput = %.1f\n", thread->resp_received*1000000.0/(now - thread->last_stats_time));
    //     thread->last_stats_time = now;
    //     thread->resp_received = 0;
    // }
    // /******* end *******/

    // Record if needed, either last in batch or all, depending in cfg:
    if (cfg.record_all_responses) {
        //printf("complete %"PRIu64" @ %"PRIu64"\n", c->complete, now);
        assert(now > c->actual_latency_start[c->complete & MAXO] );
        uint64_t actual_latency_timing = now - c->actual_latency_start[c->complete & MAXO];

        // printf("if_hit = %d, nginx-lua = %d , get = %d, find = %d, set = %d, total = %d\n", l.if_hit, l.nginx_lua, l.get, l.find, l.set, actual_latency_timing);

        hdr_record_value(thread->latency_histogram, actual_latency_timing);
        hdr_record_value(thread->real_latency_histogram, actual_latency_timing);

        int nginx_latency = 0;      
        
        uint64_t pure_nginx_time = 0;
        if(l.nginx_end > c->actual_latency_start[c->complete & MAXO])
            pure_nginx_time = l.nginx_end - c->actual_latency_start[c->complete & MAXO];
        else
            pure_nginx_time = l.nginx_lua;
    	// printf("ngx_end = %lu\n", l.nginx_end);
    	// printf("start = %lu\n",c->actual_latency_start[c->complete & MAXO]);
     //    printf("pure_ngingx_time = %lu\n", pure_nginx_time);
	    hdr_record_value(thread->real_pure_nginx_histogram, pure_nginx_time);
        hdr_record_value(thread->pure_nginx_histogram, pure_nginx_time);

        
        if (l.if_hit){
            thread->memcached_hits++;

            if (actual_latency_timing > l.nginx_lua) {
                nginx_latency = actual_latency_timing -  l.get;
                hdr_record_value(thread->nginx_lua_histogram, nginx_latency);
                hdr_record_value(thread->real_nginx_lua_histogram, nginx_latency);
            }
                
            else {
                // printf("actual_latency_timing = %d, nginx-lua = %d, get = %d\n", actual_latency_timing, l.nginx_lua, l.get);
                nginx_latency = l.nginx_lua -  l.get;
                hdr_record_value(thread->nginx_lua_histogram, nginx_latency);
                hdr_record_value(thread->real_nginx_lua_histogram, nginx_latency);
            }
                
            hdr_record_value(thread->real_nginx_proc_histogram, l.nginx_lua - l.get);
            hdr_record_value(thread->get_histogram, l.get);           
            hdr_record_value(thread->real_get_histogram, l.get);           
        }
        else {
            thread->memcached_misses++;
            // printf("nginx_lua_histogram\n");
            if (actual_latency_timing > l.nginx_lua) {
                nginx_latency = actual_latency_timing - l.get - l.find - l.set;
                hdr_record_value(thread->nginx_lua_histogram, nginx_latency);
                hdr_record_value(thread->real_nginx_lua_histogram, nginx_latency);
            }
                
            else {
                // printf("actual_latency_timing = %d, nginx-lua = %d, get = %d, set = %d, find = %d\n", actual_latency_timing, l.nginx_lua, l.get, l.get, l.find);
                nginx_latency = l.nginx_lua - l.get - l.find - l.set;
                hdr_record_value(thread->nginx_lua_histogram, nginx_latency);
                hdr_record_value(thread->real_nginx_lua_histogram, nginx_latency);
            }
                
            hdr_record_value(thread->real_nginx_proc_histogram, l.nginx_lua - l.get - l.find - l.set);
            
            hdr_record_value(thread->get_histogram, l.get);
            hdr_record_value(thread->real_get_histogram, l.get);
            
            hdr_record_value(thread->find_histogram, l.find);
            hdr_record_value(thread->real_find_histogram, l.find);

            hdr_record_value(thread->set_histogram, l.set);
            // hdr_record_value(thread->real_set_histogram, l.set);
            hdr_record_value(thread->real_get_histogram, l.set);
        }

        
        
        // fprintf(thread->ff, "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\n", actual_latency_timing, nginx_latency, l.get, l.set, l.find);
        // fflush(thread->ff);

        // pthread_mutex_lock(&lock);
        
        // fprintf(pt, "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\t" "%" PRId64 "\n", actual_latency_timing, nginx_latency, l.get, l.set, l.find);
        // fflush(pt);

        // pthread_mutex_unlock(&lock);

  
    
        thread->monitored++;
        thread->accum_latency += actual_latency_timing;

        // if (thread->monitored == thread->target) {       
        //     // if (cfg.print_realtime_latency && thread->tid == 0) {
        //     if (cfg.print_realtime_latency) {
        // 		// ++thread->monitor_event_cnt;
        // 		// fprintf(stderr, "#monitor events = %d\n", thread->monitor_event_cnt);		

        //         fprintf(thread->ff, "%" PRId64 "\t", hdr_value_at_percentile(thread->real_latency_histogram, 99));
        //         fprintf(thread->ff, "%" PRId64 "\t", hdr_value_at_percentile(thread->real_nginx_lua_histogram, 99));
        //         fprintf(thread->ff, "%" PRId64 "\t", hdr_value_at_percentile(thread->real_get_histogram, 99));
        //         fprintf(thread->ff, "%" PRId64 "\t", hdr_value_at_percentile(thread->real_find_histogram, 99));
        //         fprintf(thread->ff, "%" PRId64 "\t", hdr_value_at_percentile(thread->real_pure_nginx_histogram, 99));
        //         fprintf(thread->ff, "%" PRId64 "\n", hdr_value_at_percentile(thread->real_nginx_proc_histogram, 99));
        //         // fprintf(thread->ff, "%" PRId64 "\n", hdr_value_at_percentile(thread->real_set_histogram, 99));

        //         fflush(thread->ff);
        //     }
        //     thread->monitored = 0;
        //     thread->accum_latency = 0;
        //     hdr_reset(thread->real_latency_histogram);
        //     hdr_reset(thread->real_nginx_lua_histogram);
        //     hdr_reset(thread->real_get_histogram);
        //     hdr_reset(thread->real_find_histogram);
        //     hdr_reset(thread->real_pure_nginx_histogram);
        //     hdr_reset(thread->real_nginx_proc_histogram);
        //     // hdr_reset(thread->real_set_histogram);
        // }

        if (cfg.print_all_responses && ((thread->complete) < MAXL)) 
            raw_latency[thread->tid][thread->complete] = actual_latency_timing;
    }

    // Count all responses (including pipelined ones:)
    c->complete++;
    if (!http_should_keep_alive(parser)) {
        reconnect_socket(thread, c);
        goto done;
    }

    http_parser_init(parser, HTTP_RESPONSE);

  done:
    return 0;
}

static void socket_connected(aeEventLoop *loop, int fd, void *data, int mask) {
    connection *c = data;

    switch (sock.connect(c)) {
        case OK:    break;
        case ERROR: goto error;
        case RETRY: return;
    }

    http_parser_init(&c->parser, HTTP_RESPONSE);
    c->written = 0;

    aeCreateFileEvent(c->thread->loop, fd, AE_READABLE, socket_readable, c);

    aeCreateFileEvent(c->thread->loop, fd, AE_WRITABLE, socket_writeable, c);

    return;

  error:
    c->thread->errors.connect++;
    reconnect_socket(c->thread, c);

}

static void socket_writeable(aeEventLoop *loop, int fd, void *data, int mask) {
    connection *c = data;
    thread *thread = c->thread;

    if (!c->written) {
        uint64_t time_usec_to_wait = usec_to_next_send(c);
        if (time_usec_to_wait) {
            int msec_to_wait = round((time_usec_to_wait / 1000.0L) + 0.5);

            // Not yet time to send. Delay:
            aeDeleteFileEvent(loop, fd, AE_WRITABLE);
            aeCreateTimeEvent(
                    thread->loop, msec_to_wait, delay_request, c, NULL);
            return;
        }
    }

    if (!c->written && cfg.dynamic) {
        script_request(thread->L, &c->request, &c->length);
    }

    char  *buf = c->request + c->written;
    size_t len = c->length  - c->written;
    size_t n;


    switch (sock.write(c, buf, len, &n)) {
        case OK:    break;
        case ERROR: goto error;
        case RETRY: return;
    }
    if (!c->written) {
        c->start = time_us();
        c->actual_latency_start[c->sent & MAXO] = c->start;
        //if (c->sent) printf("sent %"PRIu64" @ %"PRIu64"\n", c->sent, c->actual_latency_start[c->sent & MAXO]-c->actual_latency_start[(c->sent-1) & MAXO]);
        //if (c->sent) printf("sent %"PRIu64" @ %"PRIu64"\n", c->sent, c->start);
        c->sent ++;
    }

    c->written += n;
    if (c->written == c->length) {
        c->written = 0;
        aeDeleteFileEvent(loop, fd, AE_WRITABLE);
        aeCreateFileEvent(thread->loop, c->fd, AE_WRITABLE, socket_writeable, c);
    }
    return;

  error:
    thread->errors.write++;
    reconnect_socket(thread, c);
}


static void socket_readable(aeEventLoop *loop, int fd, void *data, int mask) {
    connection *c = data;
    size_t n;

    do {
        switch (sock.read(c, &n)) {
            case OK:    break;
            case ERROR: goto error;
            case RETRY: return;
        }

        if (http_parser_execute(&c->parser, &parser_settings, c->buf, n) != n) goto error;
        c->thread->bytes += n;
    } while (n == RECVBUF && sock.readable(c) > 0);

    return;

  error:
    c->thread->errors.read++;
    reconnect_socket(c->thread, c);
}

static uint64_t time_us() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (t.tv_sec * 1000000) + t.tv_usec;
}

static char *copy_url_part(char *url, struct http_parser_url *parts, enum http_parser_url_fields field) {
    char *part = NULL;

    if (parts->field_set & (1 << field)) {
        uint16_t off = parts->field_data[field].off;
        uint16_t len = parts->field_data[field].len;
        part = zcalloc(len + 1 * sizeof(char));
        memcpy(part, &url[off], len);
    }

    return part;
}

static struct option longopts[] = {
    { "connections",    required_argument, NULL, 'c' },
    { "duration",       required_argument, NULL, 'd' },
    { "threads",        required_argument, NULL, 't' },
    { "script",         required_argument, NULL, 's' },
    { "header",         required_argument, NULL, 'H' },
    { "latency",        no_argument,       NULL, 'L' },
    { "batch_latency",  no_argument,       NULL, 'B' },
    { "timeout",        required_argument, NULL, 'T' },
    { "help",           no_argument,       NULL, 'h' },
    { "version",        no_argument,       NULL, 'v' },
    { "rate",           required_argument, NULL, 'R' },
    { NULL,             0,                 NULL,  0  }
};

static int parse_args(struct config *cfg, char **url, struct http_parser_url *parts, char **headers, int argc, char **argv) {
    char c, **header = headers;

    memset(cfg, 0, sizeof(struct config));
    cfg->threads     = 2;
    cfg->connections = 10;
    cfg->duration    = 10;
    cfg->timeout     = SOCKET_TIMEOUT_MS;
    cfg->rate        = 0;
    cfg->record_all_responses = true;
    cfg->print_all_responses = false;
    cfg->print_realtime_latency = false;
    cfg->dist = 0;

    // Yanqi, default monitor interval 0.1s
    cfg->stats_report_us = 100000;

    while ((c = getopt_long(argc, argv, "t:c:d:s:D:H:T:R:i:LPpBrv?", longopts, NULL)) != -1) {
        switch (c) {
            case 't':
                if (scan_metric(optarg, &cfg->threads)) return -1;
                break;
            case 'c':
                if (scan_metric(optarg, &cfg->connections)) return -1;
                break;
            case 'D':
                if (!strcmp(optarg, "fixed"))  
                    cfg->dist = 0;
                if (!strcmp(optarg, "exp")) 
                    cfg->dist = 1;
                if (!strcmp(optarg, "norm")) 
                    cfg->dist = 2;
                if (!strcmp(optarg, "zipf")) 
                    cfg->dist = 3;
                break;
            case 'd':
                if (scan_time(optarg, &cfg->duration)) return -1;
                break;
            case 's':
                cfg->script = optarg;
                break;
            case 'H':
                *header++ = optarg;
                break;
            case 'P': /* Shuang: print each requests's latency */
                cfg->print_all_responses = true;
                break;
            case 'p': /* Shuang: print avg latency every 0.2s */
                cfg->print_realtime_latency = true;
                break;
            case 'i':
                cfg->stats_report_us = (uint64_t)(1000000*atof(optarg));
                // printf("report_interval = %lu\n", cfg->stats_report_us);
                assert(cfg->stats_report_us > 0);
                break;
            case 'L':
                cfg->latency = true;
                break;
            case 'B':
                cfg->record_all_responses = false;
                break;
            case 'T':
                if (scan_time(optarg, &cfg->timeout)) return -1;
                cfg->timeout *= 1000;
                break;
            case 'R':
                if(!strcasecmp(optarg, "script")) {
                    cfg->load_by_script = true;
                    break;
                }
                if (scan_metric(optarg, &cfg->rate)) return -1;
                break;
            case 'v':
                printf("wrk %s [%s] ", VERSION, aeGetApiName());
                printf("Copyright (C) 2012 Will Glozer\n");
                break;
            case 'h':
            case '?':
            case ':':
            default:
                return -1;
        }
    }

    if (optind == argc || !cfg->threads || (!cfg->duration && !cfg->load_by_script)) return -1;

    if (!script_parse_url(argv[optind], parts)) {
        fprintf(stderr, "invalid URL: %s\n", argv[optind]);
        return -1;
    }

    if (!cfg->connections || cfg->connections < cfg->threads) {
        fprintf(stderr, "number of connections must be >= threads\n");
        return -1;
    }

    if (cfg->rate == 0 && !cfg->load_by_script) {
        fprintf(stderr,
                "Throughput MUST be specified with the --rate or -R option or in script\n");
        return -1;
    }

    *url    = argv[optind];
    *header = NULL;

    return 0;
}

static void print_stats_header() {
    printf("  Thread Stats%6s%11s%8s%12s\n", "Avg", "Stdev", "99%", "+/- Stdev");
}

static void print_units(long double n, char *(*fmt)(long double), int width) {
    char *msg = fmt(n);
    int len = strlen(msg), pad = 2;

    if (isalpha(msg[len-1])) pad--;
    if (isalpha(msg[len-2])) pad--;
    width -= pad;

    printf("%*.*s%.*s", width, width, msg, pad, "  ");

    free(msg);
}

static void print_stats(char *name, stats *stats, char *(*fmt)(long double)) {
    uint64_t max = stats->max;
    long double mean  = stats_summarize(stats);
    long double stdev = stats_stdev(stats, mean);

    printf("    %-10s", name);
    print_units(mean,  fmt, 8);
    print_units(stdev, fmt, 10);
    print_units(stats_percentile(stats, 99.0), fmt, 9);
    printf("%8.2Lf%%\n", stats_within_stdev(stats, mean, stdev, 1));
}

static void print_hdr_latency(struct hdr_histogram* histogram, const char* description) {
    long double percentiles[] = { 50.0, 75.0, 90.0, 99.0, 99.9, 99.99, 99.999, 100.0};
    printf("  Latency Distribution (HdrHistogram - %s)\n", description);
    for (size_t i = 0; i < sizeof(percentiles) / sizeof(long double); i++) {
        long double p = percentiles[i];
        int64_t n = hdr_value_at_percentile(histogram, p);
        printf("%7.3Lf%%", p);
        print_units(n, format_time_us, 10);
        printf("\n");
    }
    printf("\n%s\n", "  Detailed Percentile spectrum:");
    hdr_percentiles_print(histogram, stdout, 5, 1000.0, CLASSIC);
}

static void print_stats_latency(stats *stats) {
    long double percentiles[] = { 50.0, 75.0, 90.0, 99.0, 99.9, 99.99, 99.999, 100.0 };
    printf("  Latency Distribution\n");
    for (size_t i = 0; i < sizeof(percentiles) / sizeof(long double); i++) {
        long double p = percentiles[i];
        uint64_t n = stats_percentile(stats, p);
        printf("%7.3Lf%%", p);
        print_units(n, format_time_us, 10);
        printf("\n");
    }
}
