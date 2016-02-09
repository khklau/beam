#ifndef BEAM_DUPLEX_ENDPOINT_HXX
#define BEAM_DUPLEX_ENDPOINT_HXX

#include <beam/duplex/unordered_mixed.hpp>
#include <stdexcept>
#include <turbo/toolset/extension.hpp>

namespace beam {
namespace duplex {
namespace unordered_mixed {

namespace bdc = beam::duplex::common;
namespace bii4 = beam::internet::ipv4;
namespace bme = beam::message;

class key
{
    template <class in_connection_t, class out_connection_t>
    explicit key(const initiator<in_connection_t, out_connection_t>&)
    { }
    template <class in_connection_t, class out_connection_t>
    explicit key(const responder<in_connection_t, out_connection_t>&)
    { }
};

template <class unreliable_msg_t, class reliable_msg_t>
in_connection<unreliable_msg_t, reliable_msg_t>::in_connection(const key&, asio::io_service::strand& strand, ENetHost& host, ENetPeer& peer) :
	strand_(strand),
	host_(host),
	peer_(peer)
{ }

template <class unreliable_msg_t, class reliable_msg_t>
beam::duplex::common::identity in_connection<unreliable_msg_t, reliable_msg_t>::get_endpoint_id() const
{
    return { peer_.address.host, peer_.address.port };
}

template <class unreliable_msg_t, class reliable_msg_t>
out_connection<unreliable_msg_t, reliable_msg_t>::out_connection(const key&, asio::io_service::strand& strand, ENetHost& host, ENetPeer& peer) :
	strand_(strand),
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
void out_connection<unreliable_msg_t, reliable_msg_t>::free_message(ENetPacket* packet)
{
    delete reinterpret_cast<kj::Array<capnp::word>*>(packet->userData);
}

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::send_unreliable(beam::message::capnproto<unreliable_msg_t>& message)
{
    send(std::move(message.get_segments()), channel_id::unreliable);
}

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::send_reliable(beam::message::capnproto<reliable_msg_t>& message)
{
    send(std::move(message.get_segments()), channel_id::reliable);
}

template <class unreliable_msg_t, class reliable_msg_t>
void out_connection<unreliable_msg_t, reliable_msg_t>::send(kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> message, channel_id::type channel)
{
    kj::Array<capnp::word>* array = new kj::Array<capnp::word>(std::move(messageToFlatArray(message)));
    ENetPacket* packet = enet_packet_create(
            array->begin(),
            array->size() *  sizeof(capnp::word),
            get_packet_flags(channel));
    packet->userData = array;
    packet->freeCallback = &out_connection<unreliable_msg_t, reliable_msg_t>::free_message;
    if (TURBO_LIKELY(enet_peer_send(&peer_, channel, packet) == 0))
    {
        enet_host_flush(&host_);
    }
    else
    {
	throw std::runtime_error("Transport layer outgoing connection failed");
    }
}

perf_params::perf_params(std::size_t max, std::size_t download, std::size_t upload) :
	max_connections(max),
	download_bytes_per_sec(download),
	upload_bytes_per_sec(upload)
{ }

template <class in_connection_t, class out_connection_t>
initiator<in_connection_t, out_connection_t>::initiator(asio::io_service::strand& strand, perf_params&& params) :
	strand_(strand),
	host_(),
	peer_(),
	out_()
{
    ENetHost* host = enet_host_create(nullptr, params.max_connections, 2, params.download_bytes_per_sec, params.upload_bytes_per_sec);
    if (TURBO_UNLIKELY(host_ == nullptr))
    {
	throw std::runtime_error("Transport layer endpoint initialisation failed");
    }
    else
    {
	host_.reset(host, [](ENetHost* host)
	{
	    enet_host_destroy(host);
	});
    }
}

template <class in_connection_t, class out_connection_t>
connection_result initiator<in_connection_t, out_connection_t>::connect(std::vector<bii4::address>&& receive_candidates, bdc::port port, std::chrono::milliseconds timeout)
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
            if (enet_host_service(host_.get(), &event, timeout.count()) > 0 && event.type == ENET_EVENT_TYPE_CONNECT)
            {
                peer_.reset(peer, [](ENetPeer* peer)
		{
		    enet_peer_reset(peer);
		});
                out_.reset(new out_connection_t(key(*this), strand_, *host_, *peer_));
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
void initiator<in_connection_t, out_connection_t>::async_send(std::function<void(out_connection_t&)> callback)
{
    if (out_)
    {
	strand_.post(callback);
    }
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::async_receive(const typename in_connection_t::event_handlers& handlers)
{
    strand_.post(std::bind(
            &initiator<in_connection_t, out_connection_t>::check_events,
            this,
            handlers));
}

template <class in_connection_t, class out_connection_t>
void initiator<in_connection_t, out_connection_t>::check_events(const typename in_connection_t::event_handlers& handlers)
{
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
	    in_connection_t in(strand_, *host_, *(event.peer));
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
                    kj::ArrayPtr<capnp::word> tmp(
                            reinterpret_cast<capnp::word*>(event.packet->data),
                            event.packet->dataLength / sizeof(capnp::word));
                    if (event.channelID == channel_id::unreliable)
                    {
                        std::unique_ptr<bme::capnproto<typename in_connection_t::unreliable_msg_type>> message(
				new bme::capnproto<typename in_connection_t::unreliable_msg_type>(tmp));
                        handlers.on_receive_unreliable_msg(in, std::move(message));
                    }
                    else if (event.channelID == channel_id::reliable)
                    {
                        std::unique_ptr<bme::capnproto<typename in_connection_t::reliable_msg_type>> message(
				new bme::capnproto<typename in_connection_t::reliable_msg_type>(tmp));
                        handlers.on_receive_reliable_msg(in, std::move(message));
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
responder<in_connection_t, out_connection_t>::responder(asio::io_service::strand& strand, const beam::duplex::common::identity& id, perf_params&& params) :
	strand_(strand),
	host_(nullptr)
{
    ENetAddress address{id.address, id.port};
    host_ = enet_host_create(&address, params.max_connections, 2, params.download_bytes_per_sec, params.upload_bytes_per_sec);
    if (host_ == nullptr)
    {
	throw std::runtime_error("Transport layer endpoint initialisation failed");
    }
}

template <class in_connection_t, class out_connection_t>
responder<in_connection_t, out_connection_t>::~responder()
{
    if (host_ != nullptr)
    {
	enet_host_destroy(host_);
    }
}

} // namespace unordered_mixed
} // namespace duplex
} // namespace beam

#endif
