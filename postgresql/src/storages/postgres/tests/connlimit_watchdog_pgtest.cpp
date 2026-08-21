#include <storages/postgres/connlimit_watchdog.hpp>

#include <storages/postgres/tests/util_pgtest.hpp>

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <userver/utest/utest.hpp>

#include <storages/postgres/detail/cluster_impl.hpp>
#include <storages/postgres/detail/connection.hpp>
#include <storages/postgres/postgres_config.hpp>
#include <userver/utils/statistics/metrics_storage.hpp>

#include <userver/dynamic_config/test_helpers.hpp>

USERVER_NAMESPACE_BEGIN

namespace pg = storages::postgres;
namespace pgd = storages::postgres::detail;

namespace {

constexpr std::size_t kShardNumber = 0;

pgd::ClusterImpl CreateClusterImpl(
    const pg::DsnList& dsns,
    engine::TaskProcessor& bg_task_processor,
    size_t max_size,
    testsuite::TestsuiteTasks& testsuite_tasks,
    pg::ConnectionSettings conn_settings = kCachePreparedStatements
) {
    auto source = dynamic_config::GetDefaultSource();
    return pgd::ClusterImpl(
        dsns,
        nullptr,
        bg_task_processor,
        {{},
         {utest::kMaxTestWaitTime},
         {0, max_size, max_size},
         conn_settings,
         pg::InitMode::kAsync,
         "",
         pg::ConnlimitMode::kAuto,
         {}},
        {kTestCmdCtl, {}, {}},
        {},
        {},
        testsuite_tasks,
        source,
        std::make_shared<USERVER_NAMESPACE::utils::statistics::MetricsStorage>(),
        kShardNumber
    );
}

const std::string kWatchdogTaskName = fmt::format("connlimit_watchdog_{}_{}", "", kShardNumber);

constexpr std::string_view kRawInsert = R"(
        INSERT INTO u_clients (hostname, updated, max_connections, cur_user) VALUES
        ($1, NOW(), $2, {}) ON CONFLICT (hostname) DO UPDATE SET updated = NOW(), max_connections = $2, cur_user = {}
    )";

constexpr std::size_t kHostsCount = 10;

pg::Transaction GetTransaction(pgd::ClusterImpl& cluster) {
    static pg::CommandControl command_control{std::chrono::seconds(2), std::chrono::seconds(2)};
    return cluster.Begin({pg::ClusterHostType::kMaster}, {}, command_control);
}

constexpr size_t kReservedConn = 5;
constexpr size_t kTestsuiteServerConnlimit = 100;
constexpr size_t kTestsuiteConnlimit = kTestsuiteServerConnlimit - kReservedConn;
constexpr size_t kFallbackConnlimit = 17;
constexpr size_t kMaxStepsWithError = 3;

enum class MigrationVersion { kV1 = 0, kV2 = 1, kV3 = 2, kCount };

}  // namespace

class Watchdog : public PostgreSQLBase {
public:
    static_assert(
        static_cast<int>(MigrationVersion::kCount) == 3,
        "It is very dangerous. You must add new tests for a new migration version!"
    );

    Watchdog()
        : cluster_(CreateClusterImpl(GetDsnListFromEnv(), GetTaskProcessor(), kHostsCount * 2, testsuite_tasks_))
    {
        // Do the step of ConnlimitWatchdog to create the table.
        testsuite_tasks_.RunTask(kWatchdogTaskName);

        ClearTable();
    }

    std::size_t DoStepV1() {
        // This watchdog use the native host like the watchdog in ClusterImpl.
        auto connlimit_watchdog_v1 = MakeConnlimitWatchdog();
        connlimit_watchdog_v1.StepV1();
        return connlimit_watchdog_v1.GetConnlimit();
    }

    std::size_t DoStepV2() {
        // Use different host names to emulate different hosts.
        auto connlimit_watchdog_v2 = MakeConnlimitWatchdog("host2");
        connlimit_watchdog_v2.StepV2();
        return connlimit_watchdog_v2.GetConnlimit();
    }

    std::size_t DoStepV3() {
        auto connlimit_watchdog_v3 = MakeConnlimitWatchdog(
            "host3", [] { return std::chrono::seconds{6}; }
        );
        connlimit_watchdog_v3.StepV3();
        return connlimit_watchdog_v3.GetConnlimit();
    }

    pg::ConnlimitWatchdog MakeConnlimitWatchdog(
        std::string host_name = hostinfo::blocking::GetRealHostName(),
        pg::ConnlimitWatchdog::LoadDurationProvider load_duration_provider = {}
    ) {
        return pg::ConnlimitWatchdog{
            cluster_,
            testsuite_tasks_,
            kShardNumber,
            kFallbackConnlimit,
            [] {},
            std::move(load_duration_provider),
            std::move(host_name)
        };
    }

    pgd::ClusterImpl& GetCluster() { return cluster_; }

    void ClearTable() {
        auto t = GetTransaction(cluster_);
        t.Execute("DELETE FROM u_clients");
        t.Commit();
    }

    void InsertTicket(std::string_view host_name, std::chrono::seconds age) {
        auto t = GetTransaction(cluster_);
        t.Execute(
            R"(
                INSERT INTO u_clients (hostname, updated, max_connections, cur_user)
                VALUES ($1, NOW() - $2, 1, current_user)
            )",
            host_name,
            age
        );
        t.Commit();
    }

private:
    testsuite::TestsuiteTasks testsuite_tasks_{true};
    pgd::ClusterImpl cluster_;
    utils::impl::UserverExperimentsScope scope_;
};

template <class T>
concept HasNewVersion = requires { &T::StepV4; };

template <class T>
concept HasOldVersions = requires {
    &T::StepV1;
    &T::StepV2;
    &T::StepV3;
};

static_assert(
    !HasNewVersion<pg::ConnlimitWatchdog>,
    "Please update the following test for StepV* and increment the version check in above concept"
);

// We check different combinations of queries order with table 'u_clients', because
// services can be deployed on different versions of userver and rolled back to random version
UTEST_F(Watchdog, AllPermutations) {
    static_assert(
        HasOldVersions<pg::ConnlimitWatchdog>,
        "Do not remove old versions of StepV*, because there may be users that still use it and they may update "
        "userver one day. So we need to make sure that the update (and a rollback) will be successful."
    );
    // Fill the table with one row from every migration version.
    EXPECT_EQ(kTestsuiteConnlimit, DoStepV1());
    EXPECT_EQ(kTestsuiteConnlimit / 2, DoStepV2());
    EXPECT_EQ(kTestsuiteConnlimit / 3, DoStepV3());

    std::vector<MigrationVersion> combinations{
        MigrationVersion::kV1, MigrationVersion::kV2, MigrationVersion::kV3};
    auto do_step = [this](MigrationVersion version) {
        if (version == MigrationVersion::kV1) {
            EXPECT_EQ(kTestsuiteConnlimit / 3, DoStepV1());
        } else if (version == MigrationVersion::kV2) {
            EXPECT_EQ(kTestsuiteConnlimit / 3, DoStepV2());
        } else if (version == MigrationVersion::kV3) {
            EXPECT_EQ(kTestsuiteConnlimit / 3, DoStepV3());
        } else {
            UINVARIANT(false, "Please provide the code for this version");
        }
    };

    do {
        for (const auto version : combinations) {
            do_step(version);
        }
    } while (std::next_permutation(combinations.begin(), combinations.end()));
}

UTEST_F(Watchdog, MultiUsersWithV1) {
    DoStepV1();
    {
        auto t = GetTransaction(GetCluster());
        const auto user_name = R"('new_user')";
        t.Execute(fmt::format(kRawInsert, user_name, user_name), "new_user_host1", 7);
        t.Commit();
    }
    // StepV1 divides connections between all users => 'new_user' affects a connlimit.
    EXPECT_EQ(kTestsuiteConnlimit / 2, DoStepV1());
    {
        auto t = GetTransaction(GetCluster());
        const auto user_name = R"('new_user')";
        t.Execute(fmt::format(kRawInsert, user_name, user_name), "new_user_host2", 7);
        t.Commit();
    }
    // StepV1 divides connections between all users => 'new_user' affects a connlimit.
    EXPECT_EQ(kTestsuiteConnlimit / 3, DoStepV1());
}

UTEST_F(Watchdog, MultiUsersWithV2) {
    DoStepV2();
    {
        auto t = GetTransaction(GetCluster());
        const auto user_name = R"('new_user')";
        t.Execute(fmt::format(kRawInsert, user_name, user_name), "new_user_host1", 7);
        t.Commit();
    }
    // StepV2 divides connections only between current_user => 'new_user' doesn't affect a connlimit.
    EXPECT_EQ(kTestsuiteConnlimit, DoStepV2());
    {
        auto t = GetTransaction(GetCluster());
        const auto user_name = R"('new_user')";
        t.Execute(fmt::format(kRawInsert, user_name, user_name), "new_user_host2", 7);
        t.Commit();
    }
    EXPECT_EQ(kTestsuiteConnlimit, DoStepV2());
    {
        auto t = GetTransaction(GetCluster());
        const auto user_name = "current_user";
        // Insert the second host of 'current_user' => connlimit := connlimit / 2
        t.Execute(fmt::format(kRawInsert, user_name, user_name), "new_current_user_host", 7);
        t.Commit();
    }
    // StepV2 divides connections only between current_user => new host of 'current_user' affects a connlimit.
    EXPECT_EQ(kTestsuiteConnlimit / 2, DoStepV2());
}

UTEST_F(Watchdog, FallbackConnlimit) {
    auto expected_connlimit = kTestsuiteConnlimit;
    auto watchdog = MakeConnlimitWatchdog();
    // Do single step with working connection
    watchdog.StepV3();
    // Update connection to a non-working one
    GetCluster().SetDsnList({GetUnavailableDsn()});

    while (expected_connlimit >= kFallbackConnlimit) {
        for (size_t i = 0; i <= kMaxStepsWithError; ++i) {
            ASSERT_EQ(expected_connlimit, watchdog.GetConnlimit());
            watchdog.StepV3();
        }
        expected_connlimit /= 2;
    }

    ASSERT_EQ(kFallbackConnlimit, watchdog.GetConnlimit());
}

UTEST_F(Watchdog, CheckLimit) {
    constexpr auto
        kConnectionsLimit = kTestsuiteServerConnlimit - static_cast<std::size_t>(kTestsuiteServerConnlimit * 0.05);

    EXPECT_EQ(kConnectionsLimit, DoStepV1());

    // There are two hosts after 'StepV2'.
    EXPECT_EQ(kConnectionsLimit / 2, DoStepV2());

    // There are two hosts after 'StepV2'.
    EXPECT_EQ(kConnectionsLimit / 2, DoStepV1());

    // There are three hosts after 'StepV3'.
    EXPECT_EQ(kConnectionsLimit / 3, DoStepV3());
}

UTEST_F(Watchdog, DynamicTicketTtlKeepsSlowInstance) {
    InsertTicket("slow-instance", std::chrono::seconds{30});

    auto watchdog_v3 = MakeConnlimitWatchdog(
        "observer-v3", [] { return std::chrono::seconds{20}; }
    );
    watchdog_v3.StepV3();
    EXPECT_EQ(kTestsuiteConnlimit / 2, watchdog_v3.GetConnlimit());

    ClearTable();
    InsertTicket("slow-instance", std::chrono::seconds{30});
    auto watchdog_v2 = MakeConnlimitWatchdog("observer-v2");
    watchdog_v2.StepV2();
    EXPECT_EQ(kTestsuiteConnlimit, watchdog_v2.GetConnlimit());
}

UTEST_F(Watchdog, UsesMinTicketTtlUntilLoadDurationIsKnown) {
    InsertTicket("restarting-instance", std::chrono::seconds{20});

    std::chrono::milliseconds load_duration{};
    auto watchdog = MakeConnlimitWatchdog("observer", [&load_duration] { return load_duration; });

    watchdog.StepV3();
    EXPECT_EQ(kTestsuiteConnlimit, watchdog.GetConnlimit());

    load_duration = std::chrono::seconds{10};
    watchdog.StepV3();
    EXPECT_EQ(kTestsuiteConnlimit / 2, watchdog.GetConnlimit());
}

UTEST_F(Watchdog, CalculatesTicketTtlFromLoadDuration) {
    struct TestCase final {
        std::chrono::milliseconds load_duration;
        std::chrono::seconds ticket_age;
        bool ticket_is_alive;
    };
    constexpr TestCase kTestCases[]{
        {std::chrono::seconds{0}, std::chrono::seconds{16}, false},
        {std::chrono::seconds{4}, std::chrono::seconds{16}, false},
        {std::chrono::seconds{5}, std::chrono::seconds{16}, false},
        {std::chrono::seconds{6}, std::chrono::seconds{16}, true},
        {std::chrono::seconds{20}, std::chrono::seconds{30}, true},
    };

    for (std::size_t i = 0; i < std::size(kTestCases); ++i) {
        ClearTable();
        InsertTicket("peer", kTestCases[i].ticket_age);
        const auto host_name = fmt::format("formula-host-{}", i);
        const auto load_duration = kTestCases[i].load_duration;
        auto watchdog = MakeConnlimitWatchdog(host_name, [load_duration] {
            return load_duration;
        });

        watchdog.StepV3();
        const auto expected_instances = kTestCases[i].ticket_is_alive ? 2 : 1;
        EXPECT_EQ(kTestsuiteConnlimit / expected_instances, watchdog.GetConnlimit());
    }
}

USERVER_NAMESPACE_END
