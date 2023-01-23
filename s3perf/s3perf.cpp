#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <list>
#include <sstream>

#include <aws/auth/credentials.h>
#include <aws/common/trace_event.h>
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
#define GiB (1024LL * 1024 * 1024)
#define THROUGHPUT_TARGET_Gbps 100.0

using Clock = std::chrono::high_resolution_clock;

aws_allocator *g_alloc;
aws_event_loop_group *g_eventLoopGroup;
std::promise<void> g_eventLoopGroupDone;
aws_host_resolver *g_hostResolver;
aws_client_bootstrap *g_bootstrap;
aws_tls_ctx *g_tlsCtx;
aws_credentials_provider *g_credentialsProvider;
aws_s3_client *g_s3Client;

void OnEventLoopGroupDestroyed(void *userData) {
    g_eventLoopGroupDone.set_value();
}

void Init()
{
    g_alloc = aws_default_allocator();
    aws_s3_library_init(g_alloc);
    aws_trace_system_init(g_alloc, "trace.json");

    AWS_TRACE_EVENT_NAME_THREAD("MainThread");

    struct aws_shutdown_callback_options elgShutdownOpts = {
        .shutdown_callback_fn = OnEventLoopGroupDestroyed,
    };
    g_eventLoopGroup = aws_event_loop_group_new_default_pinned_to_cpu_group(
        g_alloc, 0 /*max-threads*/, 0 /*cpu-group*/, &elgShutdownOpts);
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

    aws_credentials_provider_chain_default_options providerOpts;
    AWS_ZERO_STRUCT(providerOpts);
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

void CleanUp() {
    g_s3Client = aws_s3_client_release(g_s3Client);
    g_credentialsProvider = aws_credentials_provider_release(g_credentialsProvider);
    aws_tls_ctx_release(g_tlsCtx);
    g_tlsCtx = NULL;
    aws_client_bootstrap_release(g_bootstrap);
    g_bootstrap = NULL;
    aws_host_resolver_release(g_hostResolver);
    g_hostResolver = NULL;
    aws_event_loop_group_release(g_eventLoopGroup);
    g_eventLoopGroup = NULL;

    g_eventLoopGroupDone.get_future().wait();

    aws_trace_system_clean_up();
    aws_thread_set_managed_join_timeout_ns(UINT64_MAX);
    aws_thread_join_all_managed();
    aws_s3_library_clean_up();
}

void AddHeader(aws_http_message *request, const std::string &name, const std::string &value)
{
    aws_http_header header = {aws_byte_cursor_from_c_str(name.c_str()), aws_byte_cursor_from_c_str(value.c_str())};
    aws_http_message_add_header(request, header);
}

class Upload
{
public:
    std::string m_filepath;
    int64_t m_fileSize;
    aws_s3_meta_request *m_metaRequest;
    std::promise<void> m_donePromise;
    std::future<void> m_doneFuture;

    Upload(const std::string &filename);
    Upload(const Upload &) = delete;

    ~Upload()
    {
        aws_s3_meta_request_release(m_metaRequest);
    }
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

    Upload *upload = static_cast<Upload *>(user_data);
    upload->m_donePromise.set_value();
}

Upload::Upload(const std::string &filepath)
    : m_filepath(filepath), m_donePromise(), m_doneFuture(m_donePromise.get_future())
{
    auto fileStream = aws_input_stream_new_from_file(g_alloc, filepath.c_str());
    AWS_FATAL_ASSERT(fileStream);
    AWS_FATAL_ASSERT(aws_input_stream_get_length(fileStream, &m_fileSize) == 0);

    auto requestPath = std::string("/") + std::filesystem::path(filepath).filename().string();

    struct aws_http_message *request = aws_http_message_new_request(g_alloc);
    aws_http_message_set_request_method(request, aws_byte_cursor_from_c_str("PUT"));
    aws_http_message_set_request_path(request, aws_byte_cursor_from_c_str(requestPath.c_str()));
    aws_http_message_set_body_stream(request, fileStream);
    AddHeader(request, "Host", std::string() + BUCKET ".s3." REGION ".amazonaws.com");
    AddHeader(request, "Content-Length", std::to_string(m_fileSize));
    AddHeader(request, "Content-Type", "application/octet-stream");

    aws_s3_meta_request_options options;
    AWS_ZERO_STRUCT(options);
    options.type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    options.message = request;
    options.user_data = this;
    options.finish_callback = OnResponseComplete;

    m_metaRequest = aws_s3_client_make_meta_request(g_s3Client, &options);
    AWS_FATAL_ASSERT(m_metaRequest);

    aws_http_message_release(request);
    aws_input_stream_release(fileStream);
}

double GetDuration(Clock::time_point startTime)
{
    return std::chrono::duration<double>(Clock::now() - startTime).count();
}

int main(int argc, char *argv[])
{
    std::string filepath = argv[1];
    int count = atoi(argv[2]);
    std::cout << "--- uploading " << filepath << "01 -> " << std::setw(2) << std::setfill('0') << count << " ---\n";

    Init();

    auto appStart = Clock::now();
    int repeatI = 0;
    double runForSec = 0.0;
    do
    {
        std::cout << "- attempt #" << ++repeatI
                  << " running for " << std::fixed << std::setprecision(3) << GetDuration(appStart) << "s -\n";

        AWS_TRACE_EVENT_BEGIN_SCOPED("s3perf", "Run");

        std::list<Upload> uploads;
        auto startTime = Clock::now();
        for (int i = 0; i < count; ++i)
        {
            std::ostringstream filepathI;
            filepathI << filepath << std::setw(2) << std::setfill('0') << (i + 1);
            uploads.emplace_back(filepathI.str());
        }

        int64_t totalBytes = 0;
        for (auto &upload : uploads)
        {
            totalBytes += upload.m_fileSize;
            upload.m_doneFuture.wait();

            if (upload.m_fileSize >= (10 * GiB))
            {
                std::cout << upload.m_filepath << ": " << std::fixed << std::setprecision(3) << GetDuration(startTime) << "s\n";
            }
        }

        AWS_TRACE_EVENT_END_SCOPED();

        auto duration = GetDuration(startTime);
        std::cout << "Total time: " << std::fixed << std::setprecision(3) << duration << "s\n";
        std::cout << "Avg per file: " << std::fixed << std::setprecision(3) << (duration / count) << "s\n";

        double gibibytes = (double)totalBytes / (double)GiB;
        std::cout << "GiB/s: " << std::fixed << std::setprecision(3) << (gibibytes / duration) << std::endl;
    } while (GetDuration(appStart) < runForSec);

    CleanUp();
    return 0;
}
