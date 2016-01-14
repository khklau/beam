#ifndef BEAM_QUEUE_UNORDERED_MIXED_HPP
#define BEAM_QUEUE_UNORDERED_MIXED_HPP

#include <cstdint>
#include <chrono>
#include <functional>
#include <string>
#include <vector>
#include <asio/io_service.hpp>
#include <asio/high_resolution_timer.hpp>
#include <beam/internet/ipv4.hpp>
#include <beam/message/capnproto.hpp>
#include <beam/queue/common.hpp>
#include <capnp/common.h>
#include <capnp/message.h>
#include <enet/enet.h>
#include <kj/array.h>

namespace beam {
namespace queue {
namespace unordered_mixed {

namespace channel_id
{
    enum type
    {
	unreliable = 0,
	reliable = 1
    };
}

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
	failure
    };
    enum class disconnection_result
    {
	success,
	not_connected,
	forced_disconnection
    };
    enum class send_result
    {
	success,
	failure,
	not_connected
    };
    struct event_handlers
    {
	std::function<void()> on_disconnect;
    };
    struct perf_params
    {
	perf_params(std::size_t win,
		std::chrono::microseconds sleep = std::chrono::microseconds(0),
		std::chrono::milliseconds connection = std::chrono::milliseconds(15000),
		std::size_t in = 0,
		std::size_t out = 0);
	std::size_t window_size;
	std::chrono::microseconds sleep_amount;
	std::chrono::milliseconds connection_timeout;
	std::size_t in_bytes_per_sec;
	std::size_t out_bytes_per_sec;
    };
    sender(asio::io_service& service, const event_handlers& handlers, const perf_params& params);
    ~sender();
    inline bool is_connected() const { return peer_ != nullptr; }
    connection_result connect(std::vector<beam::internet::ipv4::address>&& receive_candidates, beam::queue::common::port port);
    disconnection_result disconnect();
    send_result send_reliable(beam::message::capnproto<reliable_msg_t>& message);
    send_result send_unreliable(beam::message::capnproto<unreliable_msg_t>& message);
private:
    sender(const sender&) = delete;
    sender& operator=(const sender&) = delete;
    void activate();
    void deactivate();
    void on_expiry(const asio::error_code& error);
    static uint32_t get_packet_flags(channel_id::type channel);
    send_result send(kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> message, channel_id::type channel);
    static void free_message(ENetPacket* packet);
    asio::io_service& service_;
    asio::high_resolution_timer timer_;
    event_handlers handlers_;
    perf_params params_;
    ENetHost* host_;
    ENetPeer* peer_;
};

template <class unreliable_msg_t, class reliable_msg_t>
class receiver
{
public:
    typedef unreliable_msg_t unreliable_msg_type;
    typedef reliable_msg_t reliable_msg_type;
    enum class bind_result
    {
	success,
	already_bound,
	failure
    };
    struct event_handlers
    {
	std::function<void(const event_handlers& current)> on_timeout;
	std::function<void(const beam::internet::ipv4::address&, const beam::queue::common::port&)> on_connect;
	std::function<void(const beam::internet::ipv4::address&, const beam::queue::common::port&)> on_disconnect;
	std::function<void(typename beam::message::capnproto<unreliable_msg_t>&&)> on_receive_unreliable_msg;
	std::function<void(typename beam::message::capnproto<reliable_msg_t>&&)> on_receive_reliable_msg;
    };
    struct perf_params
    {
	perf_params(std::size_t connections, std::chrono::milliseconds wait = std::chrono::milliseconds(0), std::size_t in = 0, std::size_t out = 0) :
		max_connections(connections), wait_amount(wait), in_bytes_per_sec(in), out_bytes_per_sec(out)
	{ }
	std::size_t max_connections;
	std::chrono::milliseconds wait_amount;
	std::size_t in_bytes_per_sec;
	std::size_t out_bytes_per_sec;
    };
    receiver(asio::io_service& service, perf_params&& params);
    ~receiver();
    inline bool is_bound() const { return host_ != nullptr; }
    bind_result bind(const beam::queue::common::endpoint& point);
    void unbind();
    void async_receive(const event_handlers& handlers);
private:
    receiver(const receiver&) = delete;
    receiver& operator=(const receiver&) = delete;
    void check_events(const event_handlers handlers);
    asio::io_service& service_;
    perf_params params_;
    ENetHost* host_;
};

} // namespace unordered_mixed
} // namespace queue
} // namespace beam

#endif
