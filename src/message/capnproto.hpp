#ifndef BEAM_MESSAGE_CAPNPROTO_HPP
#define BEAM_MESSAGE_CAPNPROTO_HPP

#include <utility>
#include <beam/internet/endpoint.hpp>
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
class TURBO_SYMBOL_DECL statement;

template <class message_t>
class TURBO_SYMBOL_DECL payload
{
public:
    typedef message_t message_type;
    inline payload()
	:
	    buffer_(),
	    destination_(),
	    source_()
    { }
    inline payload(payload&& other)
	:
	    buffer_(std::move(other.buffer_)),
	    destination_(other.destination_),
	    source_(other.source_)
    { }
    inline payload(statement<message_type>&& other)
	:
	    buffer_(std::move(static_cast<unique_pool_ptr>(other))),
	    destination_(),
	    source_()
    { }
    inline payload(statement<message_type>&& other, beam::internet::endpoint_id destination)
	:
	    buffer_(std::move(static_cast<unique_pool_ptr>(other))),
	    destination_(destination),
	    source_()
    { }
    inline explicit payload(unique_pool_ptr&& buffer)
	:
	    buffer_(std::move(buffer)),
	    destination_(),
	    source_()
    { }
    inline payload(unique_pool_ptr&& buffer, beam::internet::endpoint_id source)
	:
	    buffer_(std::move(buffer)),
	    destination_(),
	    source_(source)
    { }
    inline payload& operator=(payload&& other)
    {
	buffer_ = std::move(other.buffer_);
	destination_ = other.destination_;
	source_ = other.source_;
	return *this;
    }
    inline explicit operator unique_pool_ptr()
    {
	return std::move(unique_pool_ptr(std::move(buffer_)));
    }
    inline beam::internet::endpoint_id get_destination() const
    {
	return destination_;
    }
    inline beam::internet::endpoint_id get_source() const
    {
	return source_;
    }
    inline void set_destination(beam::internet::endpoint_id destination)
    {
	destination_ = destination;
    }
    inline void set_source(beam::internet::endpoint_id source)
    {
	source_ = source;
    }
private:
    payload(const payload&) = delete;
    payload& operator=(const payload&) = delete;
    unique_pool_ptr buffer_;
    beam::internet::endpoint_id destination_;
    beam::internet::endpoint_id source_;
};

template <class message_t>
class TURBO_SYMBOL_DECL statement
{
public:
    typedef message_t message_type;
    explicit statement(payload<message_t>&& input);
    inline explicit operator unique_pool_ptr()
    {
	return std::move(unique_pool_ptr(std::move(buffer_)));
    }
    inline beam::internet::endpoint_id get_destination() const
    {
	return destination_;
    }
    inline beam::internet::endpoint_id get_source() const
    {
	return source_;
    }
    inline typename message_type::Reader read()
    {
	return reader_.template getRoot<message_type>();
    }
private:
    statement() = delete;
    statement(const statement&) = delete;
    statement& operator=(const statement&) = delete;
    beam::internet::endpoint_id destination_;
    beam::internet::endpoint_id source_;
    unique_pool_ptr buffer_;
    capnp::FlatArrayMessageReader reader_;
};

template <class message_t>
class TURBO_SYMBOL_DECL form
{
public:
    typedef message_t message_type;
    explicit form(unique_pool_ptr&& buffer);
    form(unique_pool_ptr&& buffer, beam::internet::endpoint_id destination);
    form(const statement<message_t>& input, unique_pool_ptr&& buffer);
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
    beam::internet::endpoint_id destination_;
    beam::internet::endpoint_id source_;
    unique_pool_ptr buffer_;
    capnp::MallocMessageBuilder builder_;
};

template <class message_t>
TURBO_SYMBOL_DECL payload<message_t> serialise(buffer_pool& pool, form<message_t>& message);

template <class message_t>
TURBO_SYMBOL_DECL void write(int fd, payload<message_t>&& payload);

template <class message_t>
TURBO_SYMBOL_DECL payload<message_t> read(int fd, std::size_t expected_word_length, buffer_pool& pool);

} // namespace capnproto
} // namespace message
} // namespace beam

#endif
