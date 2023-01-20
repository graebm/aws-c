#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <sstream>

#include <aws/auth/credentials.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/s3/s3_client.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>

#define REGION "eu-west-1"
#define BUCKET "s3perf-eu-west-1"
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
        .max_entries = 8,
        .el_group = g_eventLoopGroup,
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

    aws_credentials_provider_chain_default_options providerOpts = {0};
    providerOpts.bootstrap = g_bootstrap;
    providerOpts.tls_ctx = g_tlsCtx;
    g_credentialsProvider = aws_credentials_provider_new_chain_default(g_alloc, &providerOpts);
    AWS_FATAL_ASSERT(g_credentialsProvider);

    aws_signing_config_aws signingConfig;
    aws_s3_init_default_signing_config(&signingConfig, aws_byte_cursor_from_c_str(REGION), g_credentialsProvider);

    aws_s3_client_config s3ClientConfig = {0};
    s3ClientConfig.region = aws_byte_cursor_from_c_str(REGION);
    s3ClientConfig.client_bootstrap = g_bootstrap;
    s3ClientConfig.tls_connection_options = &tlsConnOpts;
    s3ClientConfig.signing_config = &signingConfig;
    s3ClientConfig.throughput_target_gbps = THROUGHPUT_TARGET_Gbps;
    g_s3Client = aws_s3_client_new(g_alloc, &s3ClientConfig);
    AWS_FATAL_ASSERT(g_s3Client);
}

void AddHeader(aws_http_message *request, const std::string &name, const std::string &value)
{
    aws_http_header header = {aws_byte_cursor_from_c_str(name.c_str()), aws_byte_cursor_from_c_str(value.c_str())};
    aws_http_message_add_header(request, header);
}

struct Response
{
    std::promise<void> m_donePromise;
};

void OnResponseComplete(struct aws_s3_meta_request *meta_request,
                        const struct aws_s3_meta_request_result *meta_request_result,
                        void *user_data)
{
    if (meta_request_result->error_code != 0)
    {
        printf("Failed with error_code:%s\n", aws_error_name(meta_request_result->error_code));
        if (meta_request_result->response_status)
        {
            printf("Status-Code: %d\n", meta_request_result->response_status);
        }
        auto headers = meta_request_result->error_response_headers;
        if (headers)
        {
            for (size_t i = 0; i < aws_http_headers_count(headers); ++i)
            {
                aws_http_header headerI;
                aws_http_headers_get_index(headers, i, &headerI);
                printf(PRInSTR ": " PRInSTR "\n", AWS_BYTE_CURSOR_PRI(headerI.name), AWS_BYTE_CURSOR_PRI(headerI.value));
            }
        }
        auto bodyBuf = meta_request_result->error_response_body;
        printf(PRInSTR "\n", AWS_BYTE_CURSOR_PRI(aws_byte_cursor_from_buf(bodyBuf)));
        exit(1);
    }

    Response *response = static_cast<Response *>(user_data);
    response->m_donePromise.set_value();
}

void Upload(const std::string filename)
{
    auto fileStream = aws_input_stream_new_from_file(g_alloc, filename.c_str());
    AWS_FATAL_ASSERT(fileStream);
    int64_t fileLength = 0;
    AWS_FATAL_ASSERT(aws_input_stream_get_length(fileStream, &fileLength) == 0);

    auto requestPath = std::string("/") + std::filesystem::path(filename).filename().string();

    struct aws_http_message *request = aws_http_message_new_request(g_alloc);
    aws_http_message_set_request_method(request, aws_byte_cursor_from_c_str("PUT"));
    aws_http_message_set_request_path(request, aws_byte_cursor_from_c_str(requestPath.c_str()));
    aws_http_message_set_body_stream(request, fileStream);
    AddHeader(request, "Host", std::string() + BUCKET ".s3." REGION ".amazonaws.com");
    AddHeader(request, "Content-Length", std::to_string(fileLength));
    AddHeader(request, "Content-Type", "application/octet-stream");

    Response response;
    auto doneFuture = response.m_donePromise.get_future();

    aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    options.message = request;
    options.user_data = &response;
    options.finish_callback = OnResponseComplete;

    aws_s3_meta_request *metaRequest = aws_s3_client_make_meta_request(g_s3Client, &options);
    AWS_FATAL_ASSERT(metaRequest);

    doneFuture.wait();

    aws_s3_meta_request_release(metaRequest);
    aws_http_message_release(request);
    aws_input_stream_release(fileStream);
}

int main(int argc, char *argv[])
{

    std::string filepath = argv[1];
    int repeatCount = atoi(argv[2]);
    std::cout << "--- uploading " << filepath << "01 -> " << std::setw(2) << std::setfill('0') << repeatCount << " ---\n";

    Init();

    auto startTime = Clock::now();

    for (int i = 0; i < repeatCount; ++i)
    {
        std::ostringstream filepathI;
        filepathI << filepath << std::setw(2) << std::setfill('0') << (i + 1);
        std::cout << filepathI.str() << ": ...";
        std::cout.flush();

        auto uploadStartTime = Clock::now();

        Upload(filepathI.str());

        auto durationI = std::chrono::duration<double>(Clock::now() - uploadStartTime).count();
        std::cout << "\b\b\b" << std::fixed << std::setprecision(3) << durationI << "s\n";
    }

    auto endTime = Clock::now();
    auto totalDuration = std::chrono::duration<double>(endTime - startTime).count();
    std::cout << "Total time: " << std::fixed << std::setprecision(3) << totalDuration << "s\n";
    return 0;
}
