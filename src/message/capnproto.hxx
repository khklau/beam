#ifndef BEAM_MESSAGE_CAPNPROTO_HXX
#define BEAM_MESSAGE_CAPNPROTO_HXX

#include <beam/message/capnproto.hpp>
#include <algorithm>
#include <utility>
#include <algorithm>

namespace beam {
namespace message {

template <class message_t>
capnproto<message_t>::capnproto(unique_pool_ptr&& buffer)
    :
	buffer_(std::move(buffer)),
	builder_(buffer_->asPtr())
{ }

template <class message_t>
capnproto<message_t>::capnproto(kj::ArrayPtr<capnp::word> source, unique_pool_ptr&& buffer)
    :
	capnproto(std::move(buffer))
{
    capnp::FlatArrayMessageReader reader(source);
    builder_.setRoot(reader.getRoot<message_t>());
}

template <class message_t>
buffer capnproto<message_t>::serialise()
{
    return std::move(capnp::messageToFlatArray(builder_));
}

} // namespace message
} // namespace beam

#endif
