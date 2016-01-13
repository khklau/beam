#ifndef BEAM_MESSAGE_CAPNPROTO_HXX
#define BEAM_MESSAGE_CAPNPROTO_HXX

#include <beam/message/capnproto.hpp>
#include <capnp/serialize.h>

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
capnproto<message_t>::capnproto(kj::Array<capnp::word>& input) :
	message_()
{
    capnp::FlatArrayMessageReader reader(input);
    message_.setRoot(reader.getRoot<message_t>());
}

template <class message_t>
typename message_t::Builder capnproto<message_t>::getBuilder()
{
    return message_.initRoot<message_t>();
}

template <class message_t>
typename message_t::Reader capnproto<message_t>::getReader()
{
    return message_.getRoot<message_t>().asReader();
}

template <class message_t>
kj::Array<capnp::word> capnproto<message_t>::toFlatArray()
{
    return std::move(messageToFlatArray(message_));
}

} // namespace message
} // namespace beam

#endif
