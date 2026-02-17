#pragma once
#ifdef _WIN32
#define _WIN32_WINNT	0x0601
#endif

#include "message_types.h"
#include "serialization.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <map>
#include <chrono>
#include <iostream>
#include <optional>
#include <functional>
#include "threadSafeMap.h"

namespace util
{
    inline std::string getEnvVar(const std::string& varName)
    {
        std::string returnString = std::string();
        char* env_p;

#ifdef _WIN32
        size_t requiredSize = 0;
        errno_t err = _dupenv_s(&env_p, &requiredSize, varName.c_str());

        if (err != 0 || env_p == nullptr)
        {
            return returnString;
        }

        returnString = std::string(_strdup(env_p));
        free(env_p);  // Don't forget to free the allocated memory
#else
        env_p = std::getenv(varName.c_str());
        if (env_p != nullptr)
        {
            returnString = std::string(env_p);
        }
#endif
        return returnString;
    }

#ifdef _WIN32
    inline std::string hostname = getEnvVar("COMPUTERNAME");
#else
    inline std::string hostname = getEnvVar("HOSTNAME");
#endif
    inline std::string serviceName = "RollbackServer";
    inline std::string logPrefix = "[" + hostname + "." + serviceName + "]: ";
}

namespace rollback
{
    using asio::ip::udp;
    using namespace asio::experimental::awaitable_operators;
    using namespace std::chrono;

    inline std::string logPrefix = util::logPrefix;
    //std::string hostname = "";


    // HTTP match configuration structures
    struct OVSPlayer {
        uint16_t player_index;
        std::string ip;
        bool is_host;
    };

    struct OVSMatchConfig {
        uint8_t max_players;
        uint32_t match_duration;
        std::vector<OVSPlayer> players;
    };

    // Structure to hold player information
    struct PlayerInfo
    {
        bool disconnected = false; // true if player has disconnected
        std::chrono::steady_clock::time_point lastInputTime; // Last time we received input from this player
        mutable std::shared_mutex mutex;
        asio::ip::address address;
        uint16_t port;
        std::string matchId;
        uint16_t playerIndex;
        uint32_t lastSeqRecv;
        uint32_t lastSeqSent;
        std::vector<uint32_t> ackedFrames;                    // how many frames of each player this client has acked
        bool ready;

        std::optional<time_point<steady_clock>> lastSentTime; // timestamp when we last sent a PlayerInput

        // === NEW FIELDS for ping‐smoothing and deferred rift calculation ===
        float smoothedPing = 0.0f;   // EWMA‐smoothed ping (ms)
        float smoothRift = 0.0f;
        bool  pingInitialized = false;  // Did we ever set smoothedPing at least once?
        bool  hasNewPing = false;  // Set to true whenever handlePlayerInputAck does an EWMA update.
        bool riftInit = false;

        int16_t ping = 0;

        uint32_t lastClientFrame = 0;
        bool     hasNewFrame = false; // Set to true whenever handleClientInput() updates lastClientFrame

        float rift = 0.0f;
        ThreadSafeMap<uint32_t, uint32_t>  missedInputs;
        // std::map<uint32_t, time_point<steady_clock>> pendingPings;
        ThreadSafeMap<uint32_t, time_point<steady_clock>> pendingPings;
        bool emulated;

        // --- small helper to clamp a float into ±maxRange ---
        static float clampFloat(float in, float maxRange)
        {
            if (in > maxRange) return maxRange;
            if (in < -maxRange) return -maxRange;
            return in;
        }
    };

    // Structure to hold match state
    struct MatchState
    {
        mutable std::shared_mutex mutex;
        std::string matchId;
        std::string key;
        ThreadSafeMap<std::string, std::shared_ptr<PlayerInfo>> players;
        uint32_t durationInFrames;
        float tickIntervalMs;
        uint32_t currentFrame;
        int max_players_;
        // std::vector<std::map<uint32_t, uint32_t>> inputs;     // one map per player: frame → input
        std::vector<ThreadSafeMap<uint32_t, uint32_t>> inputs;     // one map per player: frame → input

        uint32_t sequenceCounter;
        uint32_t pingPhaseCount; // how many pings sent so far
        uint32_t pingPhaseTotal; // e.g. 65

        std::atomic<bool> tickRunning;         // Signal to start/stop tick thread
        std::condition_variable tickCondition; // CV for tick thread synchronization
        std::mutex tickMutex;                  // Mutex for CV
    };

    class RollbackServer
    {
    public:
        RollbackServer(uint16_t port = GAME_SERVER_PORT, int maxPlayers = MAX_PLAYERS);
        ~RollbackServer();

        struct OVSEndpoints
        {
            const std::string RegisterPath = "/ovs_register";
            const std::string EndMatchPath = "/ovs_end_match";
        };

        struct MVSIEndpoints
        {
            const std::string RegisterPath = "/mvsi_register";
            const std::string EndMatchPath = "/mvsi_end_match";
        };

        struct URLEndpoints
        {
            const OVSEndpoints OVS;
            const MVSIEndpoints MVSI;
        };

        std::string baseURL;
        bool isOVS = false;
        bool isMVSI = false;
        URLEndpoints Endpoints;
        std::string getBaseURLFromEnv();
        void start();
        void stop();

    private:
        std::vector<std::thread> worker_threads_;

        // Network methods
        std::vector<std::shared_ptr<MatchState>> active_ping_matches_;
        std::mutex active_ping_mutex_;
        asio::awaitable<void> runUdpServer();
        asio::awaitable<void> handleMessage(
            std::vector<uint8_t> buffer,
            size_t bytesReceived,
            udp::endpoint remote);

        // Game logic methods
        std::shared_ptr<PlayerInfo> handleNewConnection(
            const NewConnectionPayload& payload,
            const udp::endpoint& remote,
            bool debug = false);

        void startPingPhase(std::shared_ptr<MatchState> match);
        asio::awaitable<void> broadcastRequestQuality(std::shared_ptr<MatchState> match);
        asio::awaitable<void> broadcastPlayersConfiguration(std::shared_ptr<MatchState> match);

        void handlePlayerInputAck(
            std::shared_ptr<MatchState> match,
            std::shared_ptr<PlayerInfo> player,
            const PlayerInputAckPayload& payload);

        void handleReady(
            std::shared_ptr<MatchState> match,
            std::shared_ptr<PlayerInfo> player,
            bool isReady);

        void handleClientInput(
            std::shared_ptr<MatchState> match,
            std::shared_ptr<PlayerInfo> player,
            const InputPayload& payload);

        void calcRiftVariableTick(
            std::shared_ptr<PlayerInfo> player,
            uint32_t serverFrame);

        void startTickLoop(std::shared_ptr<MatchState> match);
        asio::awaitable<void> runTickLoop(std::shared_ptr<MatchState> match);
        asio::awaitable<void> tick(std::shared_ptr<MatchState> match);

        asio::awaitable<void> sendPlayerInput(
            std::shared_ptr<MatchState> match,
            std::shared_ptr<PlayerInfo> player,
            const PlayerInputPayload& payload);

        asio::awaitable<uint32_t> sendServerMessage(
            std::shared_ptr<MatchState> match,
            std::shared_ptr<PlayerInfo> player,
            ServerMessageType type,
            const ServerMessageVariant& payload);

        // Fetch match config from HTTP server
        std::optional<OVSMatchConfig> fetchMatchConfigFromServer(const std::string& matchId, const std::string& key);


        void sendEndMatch(const std::string& matchId, const std::string& key);

        // Server state
        asio::io_context io_context_;
        udp::socket socket_;
        std::shared_ptr<udp::endpoint> remote_endpoint_;

        std::atomic<bool> running_;
        std::thread udp_thread_;
        std::thread tick_thread_;
  
        // std::map<std::string, std::shared_ptr<MatchState>> matches_;
        ThreadSafeMap<std::string, std::shared_ptr<MatchState>> matches_;
        ThreadSafeMap<std::string, std::shared_ptr<PlayerInfo>> players_;

    };

} // namespace rollback
