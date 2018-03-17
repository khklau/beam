#ifndef BEAM_QUEUE_UNORDERED_MIXED_HXX
#define BEAM_QUEUE_UNORDERED_MIXED_HXX

#include <cstring>
#include <tuple>
#include <capnp/common.h>
#include <capnp/serialize.h>
#include <kj/array.h>
#include <beam/message/capnproto.hxx>
#include <turbo/toolset/extension.hpp>

namespace beam {
namespace queue {
namespace unordered_mixed {

namespace bii4 = beam::internet::ipv4;
namespace bme = beam::message;
namespace bqc = beam::queue::common;

template <class unreliable_msg_t, class reliable_msg_t>
sender<unreliable_msg_t, reliable_msg_t>::sender::perf_params::perf_params(
	std::size_t window, std::chrono::microseconds sleep,
	std::chrono::milliseconds connection,
	std::size_t in,
	std::size_t out) :
    window_size(window),
    sleep_amount(sleep),
    connection_timeout(connection),
    in_bytes_per_sec(in),
    out_bytes_per_sec(out)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
sender<unreliable_msg_t, reliable_msg_t>::sender(asio::io_service::strand& strand, const event_handlers& handlers, const perf_params& params) :
	strand_(strand),
	timer_(strand_.get_io_service()),
	handlers_(handlers),
	params_(params),
	pool_(0U, params_.window_size), // FIXME: should not be empty otherwise heap allocation is needed later!
	metadata_(params_.window_size),
	host_(nullptr),
	peer_(nullptr)
{
    host_ = enet_host_create(nullptr, 1, 2, params_.in_bytes_per_sec, params_.out_bytes_per_sec);
    if (host_ == nullptr)
    {
	throw std::runtime_error("Transport layer endpoint initialisation failed");
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
sender<unreliable_msg_t, reliable_msg_t>::~sender()
{
    if (is_connected())
    {
	disconnect();
    }
    if (host_ != nullptr)
    {
	enet_host_destroy(host_);
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
typename sender<unreliable_msg_t, reliable_msg_t>::connection_result sender<unreliable_msg_t, reliable_msg_t>::connect(
	std::vector<beam::internet::ipv4::address>&& receive_candidates,
	bqc::port port)
{
    if (peer_ != nullptr)
    {
	return connection_result::already_connected;
    }
    for (auto iter = receive_candidates.begin(); iter != receive_candidates.end(); ++iter)
    {
	ENetAddress endpoint;
	endpoint.host = *iter;
	endpoint.port = port;
	peer_ = enet_host_connect(host_, &endpoint, 2U, 0U);
	if (peer_ != nullptr)
	{
	    ENetEvent event;
	    if (enet_host_service(host_, &event, params_.connection_timeout.count()) > 0 && event.type == ENET_EVENT_TYPE_CONNECT)
	    {
		activate();
		return connection_result::success;
	    }
	    else
	    {
		enet_peer_reset(peer_);
		peer_ = nullptr;
	    }
	}
    }
    return connection_result::failure;
}

template <class unreliable_msg_t, class reliable_msg_t>
typename sender<unreliable_msg_t, reliable_msg_t>::disconnection_result sender<unreliable_msg_t, reliable_msg_t>::disconnect()
{
    if (!is_connected())
    {
	return disconnection_result::not_connected;
    }
    deactivate();
    enet_peer_reset(peer_);
    peer_ = nullptr;
    return disconnection_result::forced_disconnection;
}

template <class unreliable_msg_t, class reliable_msg_t>
uint32_t sender<unreliable_msg_t, reliable_msg_t>::get_packet_flags(channel_id::type channel)
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
typename sender<unreliable_msg_t, reliable_msg_t>::send_result sender<unreliable_msg_t, reliable_msg_t>::send_unreliable(
	beam::message::payload<unreliable_msg_t>& message)
{
    return send(*(static_cast<beam::message::unique_pool_ptr>(message)), channel_id::unreliable);
}

template <class unreliable_msg_t, class reliable_msg_t>
typename sender<unreliable_msg_t, reliable_msg_t>::send_result sender<unreliable_msg_t, reliable_msg_t>::send_reliable(
	beam::message::payload<reliable_msg_t>& message)
{
    return send(*(static_cast<beam::message::unique_pool_ptr>(message)), channel_id::reliable);
}

template <class unreliable_msg_t, class reliable_msg_t>
typename sender<unreliable_msg_t, reliable_msg_t>::send_result sender<unreliable_msg_t, reliable_msg_t>::send(
	beam::message::buffer& message, channel_id::type channel)
{
    if (!is_connected())
    {
	return send_result::not_connected;
    }
    auto reservation = pool_.reserve();
    pool_[reservation] = std::move(message); // FIXME: should swap rather than assign otherwise allocation is needed again later
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
    packet->freeCallback = &sender<unreliable_msg_t, reliable_msg_t>::return_message;
    if (enet_peer_send(peer_, channel, packet) == 0)
    {
	enet_host_flush(host_);
	return send_result::success;
    }
    else
    {
	return send_result::failure;
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
void sender<unreliable_msg_t, reliable_msg_t>::activate()
{
    std::function<void(const asio::error_code&)> handler = std::bind(
	    &sender<unreliable_msg_t, reliable_msg_t>::on_expiry,
	    this,
	    std::placeholders::_1);
    timer_.expires_from_now(params_.sleep_amount);
    timer_.async_wait(strand_.wrap(handler));
}

template <class unreliable_msg_t, class reliable_msg_t>
void sender<unreliable_msg_t, reliable_msg_t>::deactivate()
{
    asio::error_code error;
    timer_.cancel(error);
}

template <class unreliable_msg_t, class reliable_msg_t>
void sender<unreliable_msg_t, reliable_msg_t>::on_expiry(const asio::error_code& error)
{
    ENetEvent event;
    if (!error && enet_host_service(host_, &event, 0) >= 0)
    {
	switch (event.type)
	{
	    case ENET_EVENT_TYPE_DISCONNECT:
	    {
		handlers_.on_disconnect(event.peer->address.host, event.peer->address.port);
		deactivate();
		break;
	    }
	    case ENET_EVENT_TYPE_NONE:
	    case ENET_EVENT_TYPE_CONNECT:
	    case ENET_EVENT_TYPE_RECEIVE:
	    {
		activate();
		break;
	    }
	}
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
void sender<unreliable_msg_t, reliable_msg_t>::return_message(ENetPacket* packet)
{
    auto metadata = static_cast<typename metadata_map_type::value_type*>(packet->userData);
    assert(metadata != nullptr);
    auto reservation = metadata->first;
    metadata->second.pool->revoke(reservation);
    metadata->second.metadata_map->erase(reservation);
}

template <class unreliable_msg_t, class reliable_msg_t>
receiver<unreliable_msg_t, reliable_msg_t>::receiver(asio::io_service::strand& strand, perf_params&& params) :
	strand_(strand),
	params_(std::move(params)),
	pool_(0, params_.window_size),
	host_(nullptr)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
receiver<unreliable_msg_t, reliable_msg_t>::~receiver()
{
    if (is_bound())
    {
	unbind();
    }
    if (host_ != nullptr)
    {
	enet_host_destroy(host_);
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
typename receiver<unreliable_msg_t, reliable_msg_t>::bind_result receiver<unreliable_msg_t, reliable_msg_t>::bind(const bqc::endpoint_id& point)
{
    if (is_bound())
    {
	return bind_result::already_bound;
    }
    ENetAddress address{point.address, point.port};
    host_ = enet_host_create(&address, params_.max_connections, 2, params_.in_bytes_per_sec, params_.out_bytes_per_sec);
    if (host_ == nullptr)
    {
	return bind_result::failure;
    }
    return bind_result::success;
}

template <class unreliable_msg_t, class reliable_msg_t>
void receiver<unreliable_msg_t, reliable_msg_t>::unbind()
{
    strand_.post(strand_.wrap(std::bind(
	    &receiver<unreliable_msg_t, reliable_msg_t>::exec_unbind,
	    this)));
}

template <class unreliable_msg_t, class reliable_msg_t>
void receiver<unreliable_msg_t, reliable_msg_t>::async_receive(const event_handlers& handlers)
{
    strand_.post(strand_.wrap(std::bind(
	    &receiver<unreliable_msg_t, reliable_msg_t>::check_events,
	    this,
	    handlers)));
}

template <class unreliable_msg_t, class reliable_msg_t>
void receiver<unreliable_msg_t, reliable_msg_t>::exec_unbind()
{
    enet_host_destroy(host_);
    host_ = nullptr;
}

template <class unreliable_msg_t, class reliable_msg_t>
void receiver<unreliable_msg_t, reliable_msg_t>::check_events(const event_handlers handlers)
{
    if (TURBO_UNLIKELY(!is_bound()))
    {
	return;
    }
    ENetEvent event;
    int occurrance = enet_host_service(host_, &event, params_.wait_amount.count());
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
		    handlers.on_disconnect(event.peer->address.host, event.peer->address.port);
		    break;
		}
		case ENET_EVENT_TYPE_CONNECT:
		{
		    handlers.on_connect(event.peer->address.host, event.peer->address.port);
		    break;
		}
		case ENET_EVENT_TYPE_RECEIVE:
		{
		    kj::ArrayPtr<capnp::word> source(
			    reinterpret_cast<capnp::word*>(event.packet->data),
			    event.packet->dataLength / sizeof(capnp::word));
		    if (event.channelID == channel_id::unreliable)
		    {
			bme::payload<unreliable_msg_type> payload(std::move(pool_.borrow_and_copy(source)));
			bme::capnproto_deed<unreliable_msg_type> message(std::move(payload));
			handlers.on_receive_unreliable_msg(message);
		    }
		    else if (event.channelID == channel_id::reliable)
		    {
			bme::payload<reliable_msg_type> payload(std::move(pool_.borrow_and_copy(source)));
			bme::capnproto_deed<reliable_msg_type> message(std::move(payload));
			handlers.on_receive_reliable_msg(message);
		    }
		    enet_packet_destroy(event.packet);
		    break;
		}
		case ENET_EVENT_TYPE_NONE:
		{
		    break;
		}
	    }
	    occurrance = enet_host_check_events(host_, &event);
	}
	while (occurrance > 0);
    }
}

} // namespace unordered_mixed
} // namespace queue
} // namespace beam

#endif

