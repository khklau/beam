#ifndef BEAM_MESSAGE_CAPNPROTO_HPP
#define BEAM_MESSAGE_CAPNPROTO_HPP

#include <utility>
#include <beam/message/buffer.hpp>
#include <capnp/common.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/array.h>
#include <kj/io.h>
#include <turbo/toolset/attribute.hpp>

namespace beam {
namespace message {

template <class message_t>
class TURBO_SYMBOL_DECL payload
{
public:
    inline explicit payload(unique_pool_ptr&& buffer) : buffer_(std::move(buffer)) { }
    inline capnp::FlatArrayMessageReader read()
    {
	return capnp::FlatArrayMessageReader(buffer_->asPtr());
    }
private:
    payload() = delete;
    payload(payload&&) = delete;
    payload& operator=(payload&&) = delete;
    unique_pool_ptr buffer_;
};

template <class message_t>
class TURBO_SYMBOL_DECL capnproto
{
public:
    typedef message_t message_type;
    explicit capnproto(unique_pool_ptr&& buffer);
    capnproto(kj::ArrayPtr<capnp::word> source, unique_pool_ptr&& buffer);
    inline typename message_type::Reader read()
    {
	return builder_.getRoot<message_type>().asReader();
    }
    inline typename message_type::Builder build()
    {
	return builder_.initRoot<message_type>();
    }
    ///
    /// Despite its claim to not require serialisation Capn Proto does need to
    /// serialise variable length segment information in front of the message.
    /// The segments required will depend on the message body content.
    /// This means the storage buffer passed to the constructor can't be used for
    /// serialisation and a new buffer needs to be allocated to store
    /// the segment info + body message.
    ///
    buffer serialise();
private:
    capnproto() = delete;
    capnproto(const capnproto&) = delete;
    capnproto& operator=(const capnproto&) = delete;
    unique_pool_ptr buffer_;
    capnp::MallocMessageBuilder builder_;
};

template <class message_t>
TURBO_SYMBOL_DECL payload<message_t> borrow_and_copy(buffer_pool& pool, kj::ArrayPtr<capnp::word> source);

} // namespace message
} // namespace beam

#endif
