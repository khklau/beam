#ifndef BEAM_QUEUE_UNORDERED_MIXED_HXX
#define BEAM_QUEUE_UNORDERED_MIXED_HXX

#include <cstring>
#include <capnp/serialize.h>

namespace beam {
namespace queue {
namespace unordered_mixed {

namespace bii4 = beam::internet::ipv4;
namespace bqc = beam::queue::common;

template <class unreliable_msg_t, class reliable_msg_t>
sender<unreliable_msg_t, reliable_msg_t>::sender::perf_params::perf_params(
	std::size_t win, std::chrono::microseconds sleep,
	std::chrono::milliseconds connection,
	std::size_t in,
	std::size_t out) :
    window_size(win),
    sleep_amount(sleep),
    connection_timeout(connection),
    in_bytes_per_sec(in),
    out_bytes_per_sec(out)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
sender<unreliable_msg_t, reliable_msg_t>::sender(asio::io_service& service, const event_handlers& handlers, const perf_params& params) :
	service_(service),
	timer_(service_),
	handlers_(handlers),
	params_(params),
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
    enet_host_destroy(host_);
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
	if (peer_)
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
typename sender<unreliable_msg_t, reliable_msg_t>::send_result sender<unreliable_msg_t, reliable_msg_t>::send_reliable(
	std::unique_ptr<capnp::MallocMessageBuilder> message)
{
    if (!is_connected())
    {
	return send_result::not_connected;
    }
    ENetPacket* packet = enet_packet_create(
	    message->getSegmentsForOutput().begin(),
	    message->getSegmentsForOutput().size(),
	    ENET_PACKET_FLAG_RELIABLE | ENET_PACKET_FLAG_NO_ALLOCATE | ENET_PACKET_FLAG_UNSEQUENCED);
    packet->userData = message.release();
    packet->freeCallback = &sender<unreliable_msg_t, reliable_msg_t>::free_reliable_msg;
    if (enet_peer_send(peer_, channel_id::reliable, packet) == 0)
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
    timer_.async_wait(handler);
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
		handlers_.on_disconnect();
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
void sender<unreliable_msg_t, reliable_msg_t>::free_reliable_msg(ENetPacket* packet)
{
    std::unique_ptr<capnp::MallocMessageBuilder>::deleter_type deleter;
    deleter(reinterpret_cast<capnp::MallocMessageBuilder*>(packet->userData)); 
}

template <class unreliable_msg_t, class reliable_msg_t>
receiver<unreliable_msg_t, reliable_msg_t>::receiver(asio::io_service& service, perf_params&& params) :
	service_(service),
	params_(std::move(params)),
	host_(nullptr)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
receiver<unreliable_msg_t, reliable_msg_t>::~receiver()
{
    if (is_bound())
    {
	unbind();
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
typename receiver<unreliable_msg_t, reliable_msg_t>::bind_result receiver<unreliable_msg_t, reliable_msg_t>::bind(const bqc::endpoint& point)
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
    enet_host_destroy(host_);
    host_ = nullptr;
}

template <class unreliable_msg_t, class reliable_msg_t>
void receiver<unreliable_msg_t, reliable_msg_t>::async_receive(const event_handlers& handlers)
{
    service_.post(std::bind(
	    &receiver<unreliable_msg_t, reliable_msg_t>::check_events,
	    this,
	    handlers));
}

template <class unreliable_msg_t, class reliable_msg_t>
void receiver<unreliable_msg_t, reliable_msg_t>::check_events(const event_handlers handlers)
{
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
		    // Can't be under sized, so round up the size to the next word
		    kj::Array<capnp::word> tmp = kj::heapArray<capnp::word>(1 + ((event.packet->dataLength - 1) / sizeof(capnp::word)));
		    memcpy(tmp.asPtr().begin(), event.packet->data, event.packet->dataLength);
		    capnp::FlatArrayMessageReader msg(tmp);
		    if (event.channelID == channel_id::unreliable)
		    {
			handlers.on_receive_unreliable_msg(msg.getRoot<unreliable_msg_t>());
		    }
		    else if (event.channelID == channel_id::reliable)
		    {
			handlers.on_receive_reliable_msg(msg.getRoot<reliable_msg_t>());
		    }
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

