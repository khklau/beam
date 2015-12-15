#ifndef BEAM_QUEUE_UNORDERED_MIXED_HXX
#define BEAM_QUEUE_UNORDERED_MIXED_HXX

namespace beam {
namespace queue {
namespace unordered_mixed {

namespace bii4 = beam::internet::ipv4;

template <class unreliable_msg_t, class reliable_msg_t>
sender<unreliable_msg_t, reliable_msg_t>::sender(asio::io_service& service, event_handlers&& handlers, perf_params&& params) :
	service_(service),
	timer_(service_),
	handlers_(handlers),
	params_(params),
	host_(nullptr),
	peer_(nullptr),
	builders_(),
	active_(),
	idle_()
{
    host_ = enet_host_create(nullptr, 1, 2, params_.in_bytes_per_sec, params_.out_bytes_per_sec);
    if (host_ == nullptr)
    {
	throw std::runtime_error("Transport layer endpoint initialisation failed");
    }
    grow(params_.window_size);
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
	port receive_port)
{
    if (peer_ != nullptr)
    {
	return connection_result::already_connected;
    }
    for (auto iter = receive_candidates.begin(); iter != receive_candidates.end(); ++iter)
    {
	ENetAddress endpoint;
	endpoint.host = *iter;
	endpoint.port = receive_port;
	peer_ = enet_host_connect(host_, &endpoint, 2U, 0U);
	if (peer_)
	{
	    activate();
	    return connection_result::success;
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
    ENetPacket* packet = register_reliable_msg(message);
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
void sender<unreliable_msg_t, reliable_msg_t>::grow(std::size_t additional_size)
{
    std::size_t new_size = builders_.size() + additional_size;
    builders_.resize(new_size);
    active_.reserve(new_size);
    std::size_t orig_size = idle_.size();
    idle_.reserve(new_size);
    for (uint32_t iter = orig_size; iter < new_size; ++iter)
    {
	idle_.emplace(iter);
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
void sender<unreliable_msg_t, reliable_msg_t>::on_expiry(const asio::error_code& error)
{
    ENetEvent event;
    if (!error && enet_host_service(host_, &event, 0) >= 0)
    {
	switch (event.type)
	{
	    case ENET_EVENT_TYPE_NONE:
	    {
		handlers_.ready();
		activate();
		break;
	    }
	    case ENET_EVENT_TYPE_DISCONNECT:
	    {
		handlers_.disconnect(event.peer->address.host);
		break;
	    }
	    case ENET_EVENT_TYPE_CONNECT:
	    case ENET_EVENT_TYPE_RECEIVE:
	    {
		// these events are useless for a sender; ignore
		activate();
	    }
	}
    }
}

template <class unreliable_msg_t, class reliable_msg_t>
ENetPacket* sender<unreliable_msg_t, reliable_msg_t>::register_reliable_msg(
	std::unique_ptr<capnp::MallocMessageBuilder> message)
{
    if (idle_.empty())
    {
	grow(builders_.size() * 0.5);
    }
    auto iter = idle_.begin();
    uint32_t index = *iter; 
    idle_.erase(iter);
    builders_.at(index) = message;
    active_.emplace(index);
    ENetPacket* packet = enet_packet_create(
	    builders_[index]->getSegmentsForOutput().begin(),
	    builders_[index]->getSegmentsForOutput().size(),
	    ENET_PACKET_FLAG_RELIABLE | ENET_PACKET_FLAG_NO_ALLOCATE | ENET_PACKET_FLAG_UNSEQUENCED);
    packet->userData = static_cast<void*>(index);
    packet->freeCallback = std::bind(
	    &sender<unreliable_msg_t, reliable_msg_t>::unregister_reliable_msg,
	    this,
	    std::placeholders::_1);
    return packet;
}

template <class unreliable_msg_t, class reliable_msg_t>
void sender<unreliable_msg_t, reliable_msg_t>::unregister_reliable_msg(const ENetPacket* packet)
{
    uint32_t index = static_cast<uint32_t>(packet->userData); 
    auto iter = active_.find(index);
    if (iter != active_.end())
    {
	active_.erase(iter);
    }
    if (index < builders_.size())
    {
	builders_[index].reset();
    }
    iter = idle_.find(index);
    if (iter == idle_.end())
    {
	idle_.emplace(index);
    }
}

} // namespace unordered_mixed
} // namespace queue
} // namespace beam

#endif

