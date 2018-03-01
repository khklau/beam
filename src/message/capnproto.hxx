#ifndef BEAM_MESSAGE_CAPNPROTO_HXX
#define BEAM_MESSAGE_CAPNPROTO_HXX

#include <beam/message/capnproto.hpp>
#include <algorithm>
#include <utility>
#include <algorithm>
#include <turbo/toolset/extension.hpp>

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

template <class message_t>
payload<message_t> borrow_and_copy(buffer_pool& pool, kj::ArrayPtr<capnp::word> source)
{
    bool copied = false;
    unique_pool_ptr buffer;
    std::size_t buffer_size = source.size();
    capnp::FlatArrayMessageReader reader(source);
    while (!copied)
    {
	buffer = std::move(pool.borrow(buffer_size));
	capnp::MallocMessageBuilder builder(buffer->asPtr());
	builder.setRoot(reader.getRoot<message_t>());
	if (TURBO_LIKELY(1U == builder.getSegmentsForOutput().size()))
	{
	    copied = true;
	}
	else
	{
	    // the borrowed buffer wasn't big enough to fit the whole message
	    buffer_size *= 2U;
	    // TODO: log a warning?
	}
    }
    payload<message_t> result(std::move(buffer));
    return std::move(result);
}

} // namespace message
} // namespace beam

#endif
