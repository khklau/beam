#ifndef BEAM_QUEUE_UNORDERED_MIXED_HPP
#define BEAM_QUEUE_UNORDERED_MIXED_HPP

#include <cstdint>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <asio/io_service.hpp>
#include <asio/high_resolution_timer.hpp>
#include <beam/internet/ipv4.hpp>
#include <capnp/message.h>
#include <enet/enet.h>

namespace beam {
namespace queue {
namespace unordered_mixed {

typedef uint16_t port;

template <class unreliable_msg_t, class reliable_msg_t>
class sender
{
public:
    typedef unreliable_msg_t unreliable_msg_type;
    typedef reliable_msg_t reliable_msg_type;
    enum class connection_result
    {
	success,
	already_connected,
	rejected,
	failure
    };
    enum class disconnection_result
    {
	success,
	not_connected,
	forced_disconnection
    };
    struct event_handlers
    {
	std::function<void()> ready;
	std::function<void(const beam::internet::ipv4::address&)> disconnect;
    };
    struct perf_params
    {
	perf_params(std::size_t win, std::chrono::microseconds sleep = std::chrono::microseconds(0), std::size_t in = 0, std::size_t out = 0) :
		window_size(win), sleep_amount(sleep), in_bytes_per_sec(in), out_bytes_per_sec(out)
	{ }
	std::size_t window_size;
	std::chrono::microseconds sleep_amount;
	std::size_t in_bytes_per_sec;
	std::size_t out_bytes_per_sec;
    };
    sender(asio::io_service& service, event_handlers&& handlers, perf_params&& params);
    ~sender();
    inline bool is_connected() const { return peer_ != nullptr; }
    connection_result connect(std::vector<beam::internet::ipv4::address>&& receive_candidates, port receive_port);
    disconnection_result disconnect();
    void grow(std::size_t additional_size);
    void send_reliable(std::unique_ptr<capnp::MallocMessageBuilder> message);
private:
    void activate();
    void on_expiry(const asio::error_code& error);
    asio::io_service& service_;
    asio::high_resolution_timer timer_;
    event_handlers handlers_;
    perf_params params_;
    ENetHost* host_;
    ENetPeer* peer_;
    std::vector<std::unique_ptr<capnp::MallocMessageBuilder>> builders_;
    std::unordered_set<uint32_t> active_;
    std::unordered_set<uint32_t> idle_;
};

template <class channel_tuple_t>
class consumer
{
public:
private:
};

} // namespace unordered_mixed
} // namespace queue
} // namespace beam

#endif
