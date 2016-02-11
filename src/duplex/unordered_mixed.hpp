#ifndef BEAM_DUPLEX_UNORDERED_MIXED_HPP
#define BEAM_DUPLEX_UNORDERED_MIXED_HPP

#include <chrono>
#include <functional>
#include <memory>
#include <unordered_map>
#include <asio/strand.hpp>
#include <beam/duplex/common.hpp>
#include <beam/internet/ipv4.hpp>
#include <beam/message/capnproto.hpp>
#include <enet/enet.h>
#include <kj/array.h>

namespace beam {
namespace duplex {
namespace unordered_mixed {

namespace channel_id
{
    enum type
    {
	unreliable = 0,
	reliable = 1
    };
}

class key;

template <class unreliable_msg_t, class reliable_msg_t>
class in_connection
{
public:
    typedef unreliable_msg_t unreliable_msg_type;
    typedef reliable_msg_t reliable_msg_type;
    struct event_handlers
    {
        std::function<void(const event_handlers& current)> on_timeout;
        std::function<void(const in_connection& connection)> on_connect;
        std::function<void(const in_connection& connection)> on_disconnect;
        std::function<void(const in_connection& connection, std::unique_ptr<beam::message::capnproto<unreliable_msg_t>>)> on_receive_unreliable_msg;
        std::function<void(const in_connection& connection, std::unique_ptr<beam::message::capnproto<reliable_msg_t>>)> on_receive_reliable_msg;
    };
    in_connection(const key&, asio::io_service::strand& strand, ENetHost& host, ENetPeer& peer);
    beam::duplex::common::identity get_endpoint_id() const;
private:
    asio::io_service::strand& strand_;
    ENetHost& host_;
    ENetPeer& peer_;
};

template <class unreliable_msg_t, class reliable_msg_t>
class out_connection
{
public:
    typedef unreliable_msg_t unreliable_msg_type;
    typedef reliable_msg_t reliable_msg_type;
    out_connection(const key&, asio::io_service::strand& strand, ENetHost& host, ENetPeer& peer);
    void send_unreliable(beam::message::capnproto<unreliable_msg_t>& message);
    void send_reliable(beam::message::capnproto<reliable_msg_t>& message);
private:
    static uint32_t get_packet_flags(channel_id::type channel);
    static void free_message(ENetPacket* packet);
    void send(kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> message, channel_id::type channel);
    asio::io_service::strand& strand_;
    ENetHost& host_;
    ENetPeer& peer_;
};

struct perf_params
{
    perf_params(std::size_t max, std::chrono::milliseconds timeout = std::chrono::milliseconds(15000), std::size_t download = 0, std::size_t upload = 0);
    std::size_t max_connections;
    std::chrono::milliseconds connection_timeout;
    std::size_t download_bytes_per_sec;
    std::size_t upload_bytes_per_sec;
};

enum class connection_result
{
    success,
    already_connected,
    failure
};

template <class in_connection_t, class out_connection_t>
class initiator
{
public:
    typedef in_connection_t in_connection_type;
    typedef out_connection_t out_connection_type;
    initiator(asio::io_service::strand& strand, perf_params&& params);
    inline bool is_connected() const { return peer_; }
    connection_result connect(std::vector<beam::internet::ipv4::address>&& receive_candidates, beam::duplex::common::port port);
    void async_send(std::function<void(out_connection_t&)> callback);
    void async_receive(const typename in_connection_t::event_handlers& handlers);
private:
    initiator() = delete;
    initiator(const initiator&) = delete;
    initiator& operator=(const initiator&) = delete;
    void exec_send(std::function<void(out_connection_t&)> callback);
    void exec_receive(const typename in_connection_t::event_handlers& handlers);
    asio::io_service::strand& strand_;
    perf_params params_;
    std::unique_ptr<ENetHost, std::function<void(ENetHost*)>> host_;
    std::unique_ptr<ENetPeer, std::function<void(ENetPeer*)>> peer_;
    std::unique_ptr<out_connection_t> out_;
};

enum class bind_result
{
    success,
    already_bound,
    failure
};

template <class in_connection_t, class out_connection_t>
class responder
{
public:
    typedef in_connection_t in_connection_type;
    typedef out_connection_t out_connection_type;
    responder(asio::io_service::strand& strand, perf_params&& params);
    inline bool is_bound() const { return host_; }
    inline bool has_connections() const { return !peer_map_.empty(); }
    bind_result bind(const beam::duplex::common::identity& id);
    void unbind();
    void async_send(const beam::duplex::common::identity& id, std::function<void(out_connection_t&)> callback);
    void async_receive(const typename in_connection_t::event_handlers& handlers);
private:
    responder() = delete;
    responder(const responder&) = delete;
    responder& operator=(const responder&) = delete;
    void exec_unbind();
    void exec_send(const beam::duplex::common::identity& id, std::function<void(out_connection_t&)> callback);
    void exec_receive(const typename in_connection_t::event_handlers& handlers);
    asio::io_service::strand& strand_;
    perf_params params_;
    std::unique_ptr<ENetHost, std::function<void(ENetHost*)>> host_;
    std::unordered_map<beam::duplex::common::identity, ENetPeer*> peer_map_;
};

} // namespace unordered_mixed
} // namespace duplex
} // namespace beam

#endif
