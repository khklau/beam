#ifndef BEAM_DUPLEX_UNORDERED_MIXED_HPP
#define BEAM_DUPLEX_UNORDERED_MIXED_HPP

#include <chrono>
#include <functional>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <asio/strand.hpp>
#include <beam/duplex/common.hpp>
#include <beam/internet/endpoint.hpp>
#include <beam/message/buffer_pool.hpp>
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

struct key;

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
        std::function<void(const in_connection& connection, beam::message::capnproto::payload<unreliable_msg_t>&& payload)> on_receive_unreliable_msg;
        std::function<void(const in_connection& connection, beam::message::capnproto::payload<reliable_msg_t>&& payload)> on_receive_reliable_msg;
    };
    in_connection(const key&, asio::io_service::strand& strand, beam::message::buffer_pool& pool, ENetHost& host, ENetPeer& peer);
    beam::internet::endpoint_id get_source_id() const;
private:
    in_connection() = delete;
    asio::io_service::strand& strand_;
    beam::message::buffer_pool& pool_;
    ENetHost& host_;
    ENetPeer& peer_;
};

template <class unreliable_msg_t, class reliable_msg_t>
class out_connection
{
public:
    struct delivery_metadata
    {
	inline delivery_metadata(
		beam::message::buffer_pool* p,
		std::unordered_map<beam::message::buffer_pool::capacity_type, delivery_metadata>* m)
	    :
		pool(p),
		metadata_map(m)
	{ }
	beam::message::buffer_pool* pool;
	std::unordered_map<beam::message::buffer_pool::capacity_type, delivery_metadata>* metadata_map;
    };
    typedef std::unordered_map<beam::message::buffer_pool::capacity_type, delivery_metadata> metadata_map_type;
    typedef unreliable_msg_t unreliable_msg_type;
    typedef reliable_msg_t reliable_msg_type;
    out_connection(
	    const key&,
	    asio::io_service::strand& strand,
	    beam::message::buffer_pool& pool,
	    metadata_map_type& metadata,
	    ENetHost& host,
	    ENetPeer& peer);
    beam::internet::endpoint_id get_destination_id() const;
    void send_unreliable(beam::message::capnproto::payload<unreliable_msg_t>& message);
    void send_reliable(beam::message::capnproto::payload<reliable_msg_t>& message);
private:
    out_connection() = delete;
    static uint32_t get_packet_flags(channel_id::type channel);
    static void return_message(ENetPacket* packet);
    void send(beam::message::buffer& message, channel_id::type channel);
    asio::io_service::strand& strand_;
    beam::message::buffer_pool& pool_;
    metadata_map_type& metadata_;
    ENetHost& host_;
    ENetPeer& peer_;
};

struct perf_params
{
    perf_params(
	    std::size_t max,
	    std::size_t window,
	    std::chrono::milliseconds timeout = std::chrono::milliseconds(15000),
	    std::size_t download = 0,
	    std::size_t upload = 0);
    std::size_t max_connections;
    std::size_t window_size;
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
    inline bool is_connected() const { return peer_.get() != nullptr; }
    connection_result connect(std::vector<beam::internet::ipv4::address>&& receive_candidates, beam::duplex::common::port port);
    void disconnect();
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
    beam::message::buffer_pool pool_;
    typename out_connection_type::metadata_map_type metadata_;
    std::unique_ptr<ENetHost, std::function<void(ENetHost*)>> host_;
    std::unique_ptr<ENetPeer, std::function<void(ENetPeer*)>> peer_;
    std::unique_ptr<out_connection_t> out_;
    // TODO add a peer_map
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
    inline bool is_bound() const { return host_.get() != nullptr; }
    inline bool has_connections() const { return !peer_map_.empty(); }
    inline beam::internet::endpoint_id get_binding() const
    {
	return is_bound()
		? beam::internet::endpoint_id(host_->address.host, host_->address.port)
		: beam::internet::endpoint_id();
    }
    bind_result bind(const beam::internet::endpoint_id& id);
    void unbind();
    void async_send(std::function<void(std::function<out_connection_t*(const beam::internet::endpoint_id&)>)> callback);
    void async_receive(const typename in_connection_t::event_handlers& handlers);
private:
    responder() = delete;
    responder(const responder&) = delete;
    responder& operator=(const responder&) = delete;
    void exec_unbind();
    void exec_send(std::function<void(std::function<out_connection_t*(const beam::internet::endpoint_id&)>)> callback);
    void exec_receive(const typename in_connection_t::event_handlers& handlers);
    asio::io_service::strand& strand_;
    perf_params params_;
    beam::message::buffer_pool pool_;
    typename out_connection_type::metadata_map_type metadata_;
    std::unique_ptr<ENetHost, std::function<void(ENetHost*)>> host_;
    std::unordered_map<beam::internet::endpoint_id, std::tuple<in_connection_t, out_connection_t>> peer_map_;
};

} // namespace unordered_mixed
} // namespace duplex
} // namespace beam

#endif
