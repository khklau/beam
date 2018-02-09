#ifndef BEAM_MESSAGE_CAPNPROTO_HXX
#define BEAM_MESSAGE_CAPNPROTO_HXX

#include <beam/message/capnproto.hpp>
#include <algorithm>
#include <utility>
#include <algorithm>

namespace {

using namespace beam::message;

kj::ArrayPtr<capnp::word> prepare_buffer(const kj::ArrayPtr<capnp::word> source, kj::Array<capnp::word>& target)
{
    if (target.size() < source.size())
    {
	// TODO: log a warning?
	std::size_t new_size = source.size();
	while (new_size < target.size())
	{
	    // FIXME: it's unlikely but we really should handle a potential overflow
	    new_size = (new_size * 3) >> 1;
	}
	target = std::move(make_buffer(new_size));
    }
    return target.asPtr();
}

} // anonymous namespace

namespace beam {
namespace message {

template <class message_t>
capnproto<message_t>::capnproto() :
	message_()
{ }

template <class message_t>
capnproto<message_t>::capnproto(kj::InputStream& input) :
	message_()
{
    capnp::readMessageCopy(input, message_);
}

template <class message_t>
capnproto<message_t>::capnproto(kj::ArrayPtr<capnp::word> flat) :
	message_()
{
    capnp::FlatArrayMessageReader reader(flat);
    message_.setRoot(reader.getRoot<message_t>());
}

template <class message_t>
typename message_t::Builder capnproto<message_t>::get_builder()
{
    return message_.initRoot<message_t>();
}

template <class message_t>
typename message_t::Reader capnproto<message_t>::get_reader()
{
    return message_.getRoot<message_t>().asReader();
}

template <class message_t>
kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> capnproto<message_t>::get_segments()
{
    return message_.getSegmentsForOutput();
}

template <class message_t>
outbound<message_t>::outbound(kj::Array<capnp::word>& buffer) :
	builder_(buffer.asPtr())
{ }

template <class message_t>
inbound<message_t>::inbound(kj::Array<capnp::word>& buffer, const kj::ArrayPtr<capnp::word> source) :
	builder_(prepare_buffer(source, buffer))
{
    capnp::FlatArrayMessageReader reader(source);
    builder_.setRoot(reader.getRoot<message_t>());
}

} // namespace message
} // namespace beam

#endif
