#ifndef BEAM_MESSAGE_CAPNPROTO_HXX
#define BEAM_MESSAGE_CAPNPROTO_HXX

#include <beam/message/capnproto.hpp>
#include <cassert>
#include <algorithm>
#include <utility>
#include <algorithm>
#include <turbo/toolset/extension.hpp>

namespace beam {
namespace message {

template <class message_t>
class key
{
    friend payload<message_t> serialise<message_t>(buffer_pool& pool, capnproto_form<message_t>& message);
};

template <class message_t>
capnproto_deed<message_t>::capnproto_deed(payload<message_t>&& source)
    :
	buffer_(std::move(static_cast<unique_pool_ptr>(source))),
	reader_(buffer_->asPtr())
{ }

template <class message_t>
capnproto_form<message_t>::capnproto_form(unique_pool_ptr&& buffer)
    :
	buffer_(std::move(buffer)),
	builder_(buffer_->asPtr())
{ }

template <class message_t>
capnproto_form<message_t>::capnproto_form(payload<message_t>&& source)
    :
	buffer_(std::move(static_cast<unique_pool_ptr>(source))),
	builder_(buffer_->asPtr())
{ }

template <class message_t>
capnproto_form<message_t>::capnproto_form(kj::ArrayPtr<capnp::word> source, unique_pool_ptr&& buffer)
    :
	capnproto_form(std::move(buffer))
{
    capnp::FlatArrayMessageReader reader(source);
    builder_.setRoot(reader.getRoot<message_t>());
}

template <class message_t>
buffer capnproto_form<message_t>::serialise()
{
    return std::move(capnp::messageToFlatArray(builder_));
}

template <class message_t>
buffer capnproto_form<message_t>::serialise(const key<message_t>&)
{
    return std::move(capnp::messageToFlatArray(builder_));
}

template <class message_t>
TURBO_SYMBOL_DECL payload<message_t> serialise(buffer_pool& pool, capnproto_form<message_t>& message)
{
    buffer buf = std::move(message.serialise(key<message_t>()));
    unique_pool_ptr ptr = std::move(pool.borrow(buf.size()));
    *ptr = std::move(buf);
    payload<message_t> result(std::move(ptr));
    return std::move(result);
}

} // namespace message
} // namespace beam

#endif
