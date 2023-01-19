#include <chrono>
#include <format>
#include <iostream>

#include <aws/auth/credentials.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/s3/s3_client.h>

#define REGION "eu-west-1"
#define KiB 1024
#define MiB (1024 * 1024)
#define THROUGHPUT_TARGET_Gbps 100.0

using Clock = std::chrono::high_resolution_clock;

aws_allocator *g_alloc;
aws_event_loop_group *g_eventLoopGroup;
aws_host_resolver *g_hostResolver;
aws_client_bootstrap *g_bootstrap;
aws_tls_ctx *g_tlsCtx;
aws_credentials_provider *g_credentialsProvider;
aws_s3_client *g_s3Client;

Clock::duration doSync(std::string filename)
{
    auto startTime = Clock::now();
    auto endTime = Clock::now();

    return endTime - startTime;
}

void Init()
{
    g_alloc = aws_default_allocator();
    aws_s3_library_init(g_alloc);

    g_eventLoopGroup = aws_event_loop_group_new_default_pinned_to_cpu_group(
        g_alloc, 0 /*max-threads*/, 0 /*cpu-group*/, NULL);
    AWS_FATAL_ASSERT(g_eventLoopGroup);

    aws_host_resolver_default_options resolverOpts = {
        .el_group = g_eventLoopGroup,
        .max_entries = 8,
    };
    g_hostResolver = aws_host_resolver_new_default(g_alloc, &resolverOpts);
    AWS_FATAL_ASSERT(g_hostResolver);

    aws_client_bootstrap_options bootstrapOpts = {
        .event_loop_group = g_eventLoopGroup,
        .host_resolver = g_hostResolver,
    };
    g_bootstrap = aws_client_bootstrap_new(g_alloc, &bootstrapOpts);

    aws_tls_ctx_options tlsCtxOpts;
    aws_tls_ctx_options_init_default_client(&tlsCtxOpts, g_alloc);
    g_tlsCtx = aws_tls_client_ctx_new(g_alloc, &tlsCtxOpts);
    AWS_FATAL_ASSERT(g_tlsCtx);

    aws_tls_connection_options tlsConnOpts;
    aws_tls_connection_options_init_from_ctx(&tlsConnOpts, g_tlsCtx);

    aws_credentials_provider_chain_default_options providerOpts = {
        .bootstrap = g_bootstrap,
        .tls_ctx = g_tlsCtx,
    };
    g_credentialsProvider = aws_credentials_provider_new_chain_default(g_alloc, &providerOpts);
    AWS_FATAL_ASSERT(g_credentialsProvider);

    aws_signing_config_aws signingConfig;
    aws_s3_init_default_signing_config(&signingConfig, aws_byte_cursor_from_c_str(REGION), g_credentialsProvider);

    aws_s3_client_config s3ClientConfig = {
        .client_bootstrap = g_bootstrap,
        .region = aws_byte_cursor_from_c_str(REGION),
        .tls_connection_options = &tlsConnOpts,
        .signing_config = &signingConfig,
        .throughput_target_gbps = THROUGHPUT_TARGET_Gbps,
    };
    g_s3Client = aws_s3_client_new(g_alloc, &s3ClientConfig);
    AWS_FATAL_ASSERT(g_s3Client);
}

int main(int argc, char *argv[])
{
    std::string filepath = argv[1];
    int repeatCount = atoi(argv[2]);
    std::cout << "uploading " << filepath << repeatCount << " times\n";
    auto appStartTime = Clock::now();
    Init();

    auto appEndTime = Clock::now();
    std::chrono::duration<double> appDurationFSec = appEndTime - appStartTime;
    printf("total app: %.3fs\n", appDurationFSec.count());
    return 0;
}
