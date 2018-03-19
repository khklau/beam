#ifndef BEAM_MESSAGE_CAPNPROTO_HPP
#define BEAM_MESSAGE_CAPNPROTO_HPP

#include <utility>
#include <beam/message/buffer.hpp>
#include <beam/message/buffer_pool.hpp>
#include <capnp/common.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/array.h>
#include <kj/io.h>
#include <turbo/toolset/attribute.hpp>

namespace beam {
namespace message {
namespace capnproto {

template <class message_t>
class TURBO_SYMBOL_DECL key;

template <class message_t>
class TURBO_SYMBOL_DECL payload
{
public:
    typedef message_t message_type;
    inline payload() : buffer_() { };
    inline payload(payload&& other) : buffer_(std::move(other.buffer_)) { };
    inline explicit payload(unique_pool_ptr&& buffer) : buffer_(std::move(buffer)) { }
    inline payload& operator=(payload&& other)
    {
	buffer_ = std::move(other.buffer_);
	return *this;
    }
    inline explicit operator unique_pool_ptr()
    {
	return std::move(unique_pool_ptr(std::move(buffer_)));
    }
private:
    payload(const payload&) = delete;
    payload& operator=(const payload&) = delete;
    unique_pool_ptr buffer_;
};

template <class message_t>
class TURBO_SYMBOL_DECL statement
{
public:
    typedef message_t message_type;
    explicit statement(payload<message_t>&& source);
    inline typename message_type::Reader read()
    {
	return reader_.getRoot<message_type>();
    }
private:
    statement() = delete;
    statement(const statement&) = delete;
    statement& operator=(const statement&) = delete;
    unique_pool_ptr buffer_;
    capnp::FlatArrayMessageReader reader_;
};

template <class message_t>
class TURBO_SYMBOL_DECL form
{
public:
    typedef message_t message_type;
    explicit form(unique_pool_ptr&& buffer);
    form(const statement<message_t>& source, unique_pool_ptr&& buffer);
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
    buffer serialise(const key<message_t>&);
private:
    form() = delete;
    form(const form&) = delete;
    form& operator=(const form&) = delete;
    unique_pool_ptr buffer_;
    capnp::MallocMessageBuilder builder_;
};

template <class message_t>
TURBO_SYMBOL_DECL payload<message_t> serialise(buffer_pool& pool, form<message_t>& message);

} // namespace capnproto
} // namespace message
} // namespace beam

#endif
