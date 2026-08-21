#include <algorithm>
#include <utility>

#include <storages/postgres/connlimit_watchdog.hpp>

#include <storages/postgres/detail/cluster_impl.hpp>
#include <userver/utils/from_string.hpp>
#include <userver/utils/impl/userver_experiments.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::postgres {

namespace {
constexpr CommandControl kCommandControl{std::chrono::seconds(2), std::chrono::seconds(2)};
constexpr std::size_t kTestsuiteConnlimit = 100;
constexpr std::size_t kMinReservedConnections = 2;
constexpr std::size_t kMaxReservedConnections = 10;
constexpr double kReservedConnectionsPercentage = 0.05;

constexpr int kMaxStepsWithError = 3;
constexpr int kBootDurationToTicketTtlMultiplier = 3;
constexpr auto kMinTicketTtl = std::chrono::milliseconds{15'000};
constexpr auto kMaxTicketTtl =
    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::microseconds::max());

std::chrono::milliseconds CalculateTicketTtl(std::chrono::milliseconds load_duration) {
    load_duration = std::max(load_duration, std::chrono::milliseconds::zero());
    if (load_duration > kMaxTicketTtl / kBootDurationToTicketTtlMultiplier) {
        return kMaxTicketTtl;
    }
    return std::max(load_duration * kBootDurationToTicketTtlMultiplier, kMinTicketTtl);
}

std::size_t GetMaxConnections(Transaction& trx) {
    const auto max_server_connections = USERVER_NAMESPACE::utils::FromString<
        ssize_t>(trx.Execute("SHOW max_connections;").AsSingleRow<std::string>());
    auto max_user_connections =
        trx.Execute("SELECT rolconnlimit FROM pg_roles WHERE rolname = current_user").AsSingleRow<ssize_t>();
    if (max_user_connections < 0) {
        max_user_connections = max_server_connections;
    }
    std::size_t max_connections = std::min(max_server_connections, max_user_connections);

    const auto reserved_connections = std::max(
        kMinReservedConnections,
        std::min(kMaxReservedConnections, static_cast<std::size_t>(max_connections * kReservedConnectionsPercentage))
    );
    if (max_connections > reserved_connections) {
        max_connections -= reserved_connections;
    } else {
        max_connections = 1;
    }
    return max_connections;
}

}  // namespace

ConnlimitWatchdog::ConnlimitWatchdog(
    detail::ClusterImpl& cluster,
    testsuite::TestsuiteTasks& testsuite_tasks,
    int shard_number,
    std::size_t min_fallback_connections,
    std::function<void()> on_new_connlimit,
    std::string host_name
)
    : ConnlimitWatchdog(
          cluster,
          testsuite_tasks,
          shard_number,
          min_fallback_connections,
          std::move(on_new_connlimit),
          {},
          std::move(host_name)
      )
{}

ConnlimitWatchdog::ConnlimitWatchdog(
    detail::ClusterImpl& cluster,
    testsuite::TestsuiteTasks& testsuite_tasks,
    int shard_number,
    std::size_t min_fallback_connections,
    std::function<void()> on_new_connlimit,
    LoadDurationProvider load_duration_provider,
    std::string host_name
)
    : cluster_(cluster),
      connlimit_(0),
      on_new_connlimit_(std::move(on_new_connlimit)),
      testsuite_tasks_(testsuite_tasks),
      shard_number_(shard_number),
      min_fallback_connections_(std::max(min_fallback_connections, kDefaultPoolMinSize)),
      load_duration_provider_(std::move(load_duration_provider)),
      host_name_(std::move(host_name))
{}

void ConnlimitWatchdog::Start() {
    try {
        auto trx = BeginTransaction();
        trx.Execute(R"(
          CREATE TABLE IF NOT EXISTS u_clients (
              hostname TEXT PRIMARY KEY,
              updated TIMESTAMPTZ NOT NULL,
              max_connections INTEGER NOT NULL
          );
        )");
        trx.Execute("ALTER TABLE u_clients ADD COLUMN IF NOT EXISTS cur_user TEXT");
        trx.Execute("ALTER TABLE u_clients SET UNLOGGED");
        // Beware! Do **not** change queries in StepV*, but rather provide a new StepV* to avoid migration issues.
        trx.Commit();
    } catch (const storages::postgres::AccessRuleViolation& e) {
        // Possible in some CREATE TABLE IF NOT EXISTS races with other services
        LOG_WARNING() << "Table already exists (not a fatal error): " << e;
    } catch (const storages::postgres::UniqueViolation& e) {
        // Possible in some CREATE TABLE IF NOT EXISTS races with other services
        LOG_WARNING() << "Table already exists (not a fatal error): " << e;
    }

    if (testsuite_tasks_.IsEnabled()) {
        connlimit_ = kTestsuiteConnlimit;
        testsuite_tasks_
            .RegisterTask(fmt::format("connlimit_watchdog_{}_{}", cluster_.GetDbName(), shard_number_), [this] {
                StepV3();
            });
    } else {
        periodic_.Start(
            "connlimit_watchdog",
            {std::chrono::seconds(2), {}, {USERVER_NAMESPACE::utils::PeriodicTask::Flags::kNow}},
            [this] { StepV3(); }
        );
    }
}

void ConnlimitWatchdog::StepV1() {
    static const Query kUpsertClientMaxConnections{
        R"(
        INSERT INTO u_clients (hostname, updated, max_connections)
        VALUES ($1, NOW(), $2)
        ON CONFLICT (hostname) DO UPDATE SET updated = NOW(), max_connections = $2
    )",
        "UpsertMaxConnectionsV1"
    };

    static const Query kSelectInstances{
        R"(
        SELECT count(*) FROM u_clients WHERE updated >= NOW() - make_interval(secs => 15)
    )",
        "SelectInstancesV1"
    };

    static auto hostname = hostinfo::blocking::GetRealHostName();

    DoStep(
        [&](Transaction& trx) {
            trx.Execute(kUpsertClientMaxConnections, hostname, static_cast<int>(GetConnlimit()));
        },
        [&](Transaction& trx) { return trx.Execute(kSelectInstances).AsSingleRow<int>(); }
    );
}

void ConnlimitWatchdog::StepV2() {
    static const Query kUpsertClientMaxConnections{
        R"(
              INSERT INTO u_clients (hostname, updated, max_connections, cur_user)
              VALUES ($1, NOW(), $2, current_user)
              ON CONFLICT (hostname) DO UPDATE SET updated = NOW(), max_connections = $2, cur_user = current_user
            )",
        "UpsertMaxConnectionsV2"
    };

    static const Query kSelectInstances{
        R"(
            SELECT count(*) FROM u_clients WHERE updated >= NOW() - make_interval(secs => 15) AND (cur_user = current_user OR cur_user is NULL)
            )",
        "SelectInstancesV2"
    };

    DoStep(
        [&](Transaction& trx) {
            trx.Execute(kUpsertClientMaxConnections, host_name_, static_cast<int>(GetConnlimit()));
        },
        [&](Transaction& trx) { return trx.Execute(kSelectInstances).AsSingleRow<int>(); }
    );
}

void ConnlimitWatchdog::StepV3() {
    static const Query kUpsertClientMaxConnections{
        R"(
            INSERT INTO u_clients (hostname, updated, max_connections, cur_user)
            VALUES ($1, NOW(), $2, current_user)
            ON CONFLICT (hostname) DO UPDATE SET updated = NOW(), max_connections = $2, cur_user = current_user
        )",
        "UpsertMaxConnectionsV3"
    };

    static const Query kSelectInstances{
        R"(
            SELECT count(*) FROM u_clients
            WHERE updated >= NOW() - $1 AND (cur_user = current_user OR cur_user is NULL)
        )",
        "SelectInstancesV3"
    };

    const auto load_duration =
        load_duration_provider_ ? load_duration_provider_() : std::chrono::milliseconds::zero();
    const auto ticket_ttl = CalculateTicketTtl(load_duration);

    DoStep(
        [&](Transaction& trx) {
            trx.Execute(kUpsertClientMaxConnections, host_name_, static_cast<int>(GetConnlimit()));
        },
        [&](Transaction& trx) { return trx.Execute(kSelectInstances, ticket_ttl).AsSingleRow<int>(); }
    );
}

void ConnlimitWatchdog::Stop() { periodic_.Stop(); }

std::size_t ConnlimitWatchdog::GetConnlimit() const noexcept { return connlimit_.load(); }

void ConnlimitWatchdog::DoStep(
    USERVER_NAMESPACE::utils::function_ref<void(Transaction&)> upsert_ticket,
    USERVER_NAMESPACE::utils::function_ref<int(Transaction&)> count_instances
) {
    try {
        auto trx = BeginTransaction();

        const auto max_connections = GetMaxConnections(trx);

        upsert_ticket(trx);
        const auto instances = count_instances(trx);

        UpdateConnectionsLimit(max_connections, instances);

        trx.Commit();
        steps_with_errors_ = 0;
    } catch (const Error& e) {
        if (++steps_with_errors_ > kMaxStepsWithError) {
            /*
             * Something's wrong with PG server. Try to lower the load by lowering
             * max connection to a small value. Active connections will be gracefully
             * closed. When the server returns the response, we'll get the real
             * connlimit value. The period with "too low max_connections" should be
             * relatively small.
             */
            const auto previous_connlimit = connlimit_.load();
            connlimit_ = std::max(previous_connlimit / 2, min_fallback_connections_);
            steps_with_errors_ = 0;
        }
        LOG_WARNING() << fmt::format("Can't connect to u_clients. Fallback max_size to {}. ", connlimit_.load()) << e;
    }

    on_new_connlimit_();
}

Transaction ConnlimitWatchdog::BeginTransaction() {
    return cluster_.Begin({ClusterHostType::kMaster}, {}, kCommandControl);
}

void ConnlimitWatchdog::UpdateConnectionsLimit(std::size_t max_connections, std::size_t instances) {
    if (instances == 0) {
        instances = 1;
    }

    auto new_connlimit = max_connections / instances;
    if (new_connlimit == 0) {
        new_connlimit = 1;
    }
    auto previous_connlimit = connlimit_.exchange(new_connlimit);
    LOG((previous_connlimit == new_connlimit) ? logging::Level::kDebug : logging::Level::kWarning
    ) << "max_connections = "
      << max_connections << ", instances = " << instances << ", connlimit = " << new_connlimit;
}

}  // namespace storages::postgres

USERVER_NAMESPACE_END
