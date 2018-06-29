#ifndef BEAM_DUPLEX_ENDPOINT_HXX
#define BEAM_DUPLEX_ENDPOINT_HXX

#include <beam/duplex/unordered_mixed.hpp>
#include <stdexcept>
#include <beam/message/capnproto.hxx>
#include <capnp/serialize.h>
#include <turbo/toolset/extension.hpp>

namespace beam {
namespace duplex {
namespace unordered_mixed {

namespace bdc = beam::duplex::common;
namespace bii4 = beam::internet::ipv4;
namespace bmc = beam::message::capnproto;

struct key
{
    template <class in_connection_t, class out_connection_t>
    explicit key(const initiator<in_connection_t, out_connection_t>&)
    { }
    template <class in_connection_t, class out_connection_t>
    explicit key(const responder<in_connection_t, out_connection_t>&)
    { }
};

template <class unreliable_msg_t, class reliable_msg_t>
in_connection<unreliable_msg_t, reliable_msg_t>::in_connection(
	const key&,
	asio::io_service::strand& strand,
	beam::message::buffer_pool& pool,
	ENetHost& host,
	ENetPeer& peer)
    :
	strand_(strand),
	pool_(pool),
	host_(host),
	peer_(peer)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
beam::internet::ipv4::endpoint_id in_connection<unreliable_msg_t, reliable_msg_t>::get_endpoint_id() const
{
    return { peer_.address.host, peer_.address.port };
}

template <class unreliable_msg_t, class reliable_msg_t>
out_connection<unreliable_msg_t, reliable_msg_t>::out_connection(
	const key&,
	asio::io_service::strand& strand,
	beam::message::buffer_pool& pool,
	metadata_map_type& metadata,
	ENetHost& host,
	ENetPeer& peer)
    :
	strand_(strand),
	pool_(pool),
	metadata_(metadata),
	host_(host),
	peer_(peer)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
uint32_t out_connection<unreliable_msg_t, reliable_msg_t>::get_packet_flags(channel_id::type channel)
{
    uint32_t flags = 0;
    switch (channel)
    {
        case channel_id::unreliable:
            flags = ENET_PACKET_FLAG_NO_ALLOCATE | ENET_PACKET_FLAG_UNSEQUENCED;
            break;
        case channel_id::reliable:
            flags = ENET_PACKET_FLAG_RELIABLE | ENET_PACKET_FLAG_NO_ALLOCATE | ENET_PACKET_FLAG_UNSEQUENCED;
            break;
    };  
    return flags;
}   

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::return_message(ENetPacket* packet)
{
    auto metadata = static_cast<typename metadata_map_type::value_type*>(packet->userData);
    assert(metadata != nullptr);
    auto reservation = metadata->first;
    metadata->second.pool->revoke(reservation);
    metadata->second.metadata_map->erase(reservation);
}

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::send_unreliable(bmc::payload<unreliable_msg_t>& message)
{
    send(*(static_cast<beam::message::unique_pool_ptr>(message)), channel_id::unreliable);
}

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::send_reliable(bmc::payload<reliable_msg_t>& message)
{
    send(*(static_cast<beam::message::unique_pool_ptr>(message)), channel_id::reliable);
}

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::send(beam::message::buffer& message, channel_id::type channel)
{
    auto reservation = pool_.reserve();
    pool_[reservation] = std::move(message);
    auto emplace_result = metadata_.emplace(
	    std::piecewise_construct,
	    std::make_tuple(reservation),
	    std::make_tuple(&pool_, &metadata_));
    assert(emplace_result.second);
    ENetPacket* packet = enet_packet_create(
            pool_[reservation].begin(),
            pool_[reservation].size() *  sizeof(capnp::word),
            get_packet_flags(channel));
    packet->userData = &(*(emplace_result.first));
    packet->freeCallback = &out_connection<unreliable_msg_t, reliable_msg_t>::return_message;
    if (TURBO_LIKELY(enet_peer_send(&peer_, channel, packet) == 0))
    {
        enet_host_flush(&host_);
    }
    else
    {
	throw std::runtime_error("Transport layer outgoing connection failed");
    }
}

perf_params::perf_params(
	std::size_t max,
	std::size_t window,
	std::chrono::milliseconds timeout,
	std::size_t download,
	std::size_t upload)
    :
	max_connections(max),
	window_size(window),
	connection_timeout(timeout),
	download_bytes_per_sec(download),
	upload_bytes_per_sec(upload)
{ }

template <class in_connection_t, class out_connection_t>
initiator<in_connection_t, out_connection_t>::initiator(asio::io_service::strand& strand, perf_params&& params) :
	strand_(strand),
	params_(std::move(params)),
	pool_(0U, params_.window_size),
	metadata_(params_.window_size),
	host_(nullptr, [](ENetHost* host) { enet_host_destroy(host); }),
	peer_(nullptr, [](ENetPeer* peer) { enet_peer_reset(peer); }),
	out_()
{
    ENetHost* host = enet_host_create(nullptr, params.max_connections, 2, params.download_bytes_per_sec, params.upload_bytes_per_sec);
    if (TURBO_UNLIKELY(host == nullptr))
    {
	throw std::runtime_error("Transport layer endpoint initialisation failed");
    }
    else
    {
	host_.reset(host);
    }
}

template <class in_connection_t, class out_connection_t>
connection_result initiator<in_connection_t, out_connection_t>::connect(std::vector<bii4::address>&& receive_candidates, bdc::port port)
{
    if (out_)
    {
        return connection_result::already_connected;
    }
    for (auto iter = receive_candidates.begin(); iter != receive_candidates.end(); ++iter)
    {
        ENetAddress endpoint;
        endpoint.host = *iter;
        endpoint.port = port;
        ENetPeer* peer = enet_host_connect(host_.get(), &endpoint, 2U, 0U);
        if (peer != nullptr)
        {
            ENetEvent event;
            if (enet_host_service(host_.get(), &event, params_.connection_timeout.count()) > 0 && event.type == ENET_EVENT_TYPE_CONNECT)
            {
                peer_.reset(peer);
                out_.reset(new out_connection_t(key(*this), strand_, pool_, metadata_, *host_, *peer_));
                return connection_result::success;
            }
            else
            {
                enet_peer_reset(peer);
            }
        }
    }
    return connection_result::failure;
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::disconnect()
{
    out_.reset();
    peer_.reset();
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::async_send(std::function<void(out_connection_t&)> callback)
{
    strand_.post(std::bind(&initiator<in_connection_t, out_connection_t>::exec_send, this, callback));
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::async_receive(const typename in_connection_t::event_handlers& handlers)
{
    strand_.post(std::bind(&initiator<in_connection_t, out_connection_t>::exec_receive, this, handlers));
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::exec_send(std::function<void(out_connection_t&)> callback)
{
    if (TURBO_LIKELY(out_.get() != nullptr))
    {
	callback(*out_);
    }
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::exec_receive(const typename in_connection_t::event_handlers& handlers)
{
    if (TURBO_UNLIKELY(!host_))
    {
	return;
    }
    ENetEvent event;
    int occurrance = enet_host_service(host_.get(), &event, 0);
    if (occurrance == 0)
    {
        handlers.on_timeout(handlers);
    }
    else
    {
        do
        {
	    in_connection_t in(key(*this), strand_, pool_, *host_, *(event.peer));
            switch (event.type)
            {
                case ENET_EVENT_TYPE_DISCONNECT:
                {
                    handlers.on_disconnect(in);
                    break;
                }
                case ENET_EVENT_TYPE_CONNECT:
                {
                    handlers.on_connect(in);
                    break;
                }
                case ENET_EVENT_TYPE_RECEIVE:
                {
                    kj::ArrayPtr<capnp::word> source(
                            reinterpret_cast<capnp::word*>(event.packet->data),
                            event.packet->dataLength / sizeof(capnp::word));
                    if (event.channelID == channel_id::unreliable)
                    {
			bmc::payload<typename in_connection_t::unreliable_msg_type> payload(std::move(pool_.borrow_and_copy(source)));
                        handlers.on_receive_unreliable_msg(in, std::move(payload));
                    }
                    else if (event.channelID == channel_id::reliable)
                    {
			bmc::payload<typename in_connection_t::reliable_msg_type> payload(std::move(pool_.borrow_and_copy(source)));
                        handlers.on_receive_reliable_msg(in, std::move(payload));
                    }
                    enet_packet_destroy(event.packet);
                    break;
                }
                case ENET_EVENT_TYPE_NONE:
                {
                    break;
                }
            }
            occurrance = enet_host_check_events(host_.get(), &event);
        }
        while (occurrance > 0);
    }
}

template <class in_connection_t, class out_connection_t>
responder<in_connection_t, out_connection_t>::responder(asio::io_service::strand& strand, perf_params&& params) :
	strand_(strand),
	params_(std::move(params)),
	pool_(0U, params_.window_size),
	metadata_(params_.window_size),
	host_(nullptr, [](ENetHost* host) { enet_host_destroy(host); }),
	peer_map_()
{
    peer_map_.reserve(params.max_connections);
}

template <class in_connection_t, class out_connection_t>
bind_result responder<in_connection_t, out_connection_t>::bind(const beam::internet::ipv4::endpoint_id& id)
{
    if (is_bound())
    {
	return bind_result::already_bound;
    }
    ENetAddress address{id.get_address(), id.get_port()};
    ENetHost* host = enet_host_create(&address, params_.max_connections, 2, params_.download_bytes_per_sec, params_.upload_bytes_per_sec);
    if (host == nullptr)
    {
	return bind_result::failure;
    }
    else
    {
	host_.reset(host);
	return bind_result::success;
    }
}

template <class in_connection_t, class out_connection_t>
void responder<in_connection_t, out_connection_t>::unbind()
{
    strand_.post(std::bind(&responder<in_connection_t, out_connection_t>::exec_unbind, this));
}

template <class in_connection_t, class out_connection_t>
void responder<in_connection_t, out_connection_t>::async_send(std::function<void(std::function<out_connection_t*(const beam::internet::ipv4::endpoint_id&)>)> callback)
{
    strand_.post(std::bind(&responder<in_connection_t, out_connection_t>::exec_send, this, callback));
}

template <class in_connection_t, class out_connection_t>
void responder<in_connection_t, out_connection_t>::async_receive(const typename in_connection_t::event_handlers& handlers)
{
    strand_.post(std::bind(&responder<in_connection_t, out_connection_t>::exec_receive, this, handlers));
}

template <class in_connection_t, class out_connection_t>
void responder<in_connection_t, out_connection_t>::exec_unbind()
{
    host_.reset();
}

template <class in_connection_t, class out_connection_t>
void responder<in_connection_t, out_connection_t>::exec_send(std::function<void(std::function<out_connection_t*(const beam::internet::ipv4::endpoint_id&)>)> callback)
{
    if (TURBO_UNLIKELY(!host_))
    {
	return;
    }
    callback([&](const beam::internet::ipv4::endpoint_id& endpoint)
    {
	auto iter = peer_map_.find(endpoint);
	if (iter != peer_map_.end())
	{
	    return &(std::get<1>(iter->second));
	}
	else
	{
	    return static_cast<out_connection_t*>(nullptr);
	}
    });
}

template <class in_connection_t, class out_connection_t>
void responder<in_connection_t, out_connection_t>::exec_receive(const typename in_connection_t::event_handlers& handlers)
{
    if (TURBO_UNLIKELY(!host_))
    {
	return;
    }
    ENetEvent event;
    int occurrance = enet_host_service(host_.get(), &event, 0);
    if (occurrance == 0)
    {
        handlers.on_timeout(handlers);
    }
    else
    {
        do
        {
            switch (event.type)
            {
                case ENET_EVENT_TYPE_DISCONNECT:
                {
		    bii4::endpoint_id id(event.peer->address.host, event.peer->address.port);
		    auto iter = peer_map_.find(id);
		    if (iter != peer_map_.end())
		    {
			handlers.on_disconnect(std::get<0>(iter->second));
			peer_map_.erase(iter);
		    }
                    break;
                }
                case ENET_EVENT_TYPE_CONNECT:
                {
		    bii4::endpoint_id id(event.peer->address.host, event.peer->address.port);
		    auto result = peer_map_.emplace(std::move(id), std::make_tuple(
			    in_connection_t(key(*this), strand_, pool_, *host_, *(event.peer)),
			    out_connection_t(key(*this), strand_, pool_, metadata_, *host_, *(event.peer))));
		    handlers.on_connect(std::get<0>(result.first->second));
                    break;
                }
                case ENET_EVENT_TYPE_RECEIVE:
                {
		    bii4::endpoint_id id(event.peer->address.host, event.peer->address.port);
		    auto iter = peer_map_.find(id);
		    if (iter == peer_map_.end())
		    {
			break;
		    }
                    kj::ArrayPtr<capnp::word> source(
                            reinterpret_cast<capnp::word*>(event.packet->data),
                            event.packet->dataLength / sizeof(capnp::word));
                    if (event.channelID == channel_id::unreliable)
                    {
			bmc::payload<typename in_connection_t::unreliable_msg_type> payload(std::move(pool_.borrow_and_copy(source)));
                        handlers.on_receive_unreliable_msg(std::get<0>(iter->second), std::move(payload));
                    }
                    else if (event.channelID == channel_id::reliable)
                    {
			bmc::payload<typename in_connection_t::reliable_msg_type> payload(std::move(pool_.borrow_and_copy(source)));
                        handlers.on_receive_reliable_msg(std::get<0>(iter->second), std::move(payload));
                    }
                    enet_packet_destroy(event.packet);
                    break;
                }
                case ENET_EVENT_TYPE_NONE:
                {
                    break;
                }
            }
            occurrance = enet_host_check_events(host_.get(), &event);
        }
        while (occurrance > 0);
    }
}

} // namespace unordered_mixed
} // namespace duplex
} // namespace beam

#endif
