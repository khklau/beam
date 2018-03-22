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
namespace capnproto {

template <class message_t>
class key
{
    friend payload<message_t> serialise<message_t>(buffer_pool& pool, form<message_t>& message);
};

template <class message_t, class reader_t>
statement<message_t, reader_t>::statement(payload<message_t>&& source)
    :
	buffer_(std::move(static_cast<unique_pool_ptr>(source))),
	reader_(buffer_->asPtr())
{ }

template <class message_t, class reader_t>
statement<message_t, reader_t>::statement(reader_type&& reader, unique_pool_ptr&& buffer)
    :
	buffer_(std::move(buffer)),
	reader_(std::move(reader))
{ }

template <class message_t>
form<message_t>::form(unique_pool_ptr&& buffer)
    :
	buffer_(std::move(buffer)),
	builder_(buffer_->asPtr())
{ }

template <class message_t>
form<message_t>::form(const statement<message_t>& source, unique_pool_ptr&& buffer)
    :
	form(std::move(buffer))
{
    builder_.setRoot(source.read());
}

template <class message_t>
buffer form<message_t>::serialise()
{
    return std::move(capnp::messageToFlatArray(builder_));
}

template <class message_t>
buffer form<message_t>::serialise(const key<message_t>&)
{
    return std::move(capnp::messageToFlatArray(builder_));
}

template <class message_t>
payload<message_t> serialise(buffer_pool& pool, form<message_t>& message)
{
    buffer buf = std::move(message.serialise(key<message_t>()));
    unique_pool_ptr ptr = std::move(pool.borrow(buf.size()));
    *ptr = std::move(buf);
    payload<message_t> result(std::move(ptr));
    return std::move(result);
}

template <class message_t>
TURBO_SYMBOL_DECL void write(int fd, const payload<message_t>& payload)
{
    // FIXME: use the kj::ExceptionCallback instead once we figure out how to register it!
    const kj::ArrayPtr<const typename capnp::word> ptr(static_cast<beam::message::unique_pool_ptr>(payload)->asPtr());
    try
    {
	capnp::writeMessageToFd(fd, kj::arrayPtr(&ptr, 1U));
    }
    catch (kj::Exception& ex)
    {
	// FIXME: retry for recoverable errors, but kj::FdOutputStream::write doesn't report the errno from the syscall
	//        so we can't tell if the error is recoverable; ignore errors for now
    }
}

template <class message_t>
streamed_statement<message_t> read(int fd, std::size_t expected_word_length, buffer_pool& pool)
{
    unique_pool_ptr ptr = std::move(pool.borrow(expected_word_length));
    capnp::StreamFdMessageReader reader(fd, capnp::ReaderOptions(), ptr->asPtr());
    statement<message_t, capnp::StreamFdMessageReader> result(std::move(reader), std::move(ptr));
    return std::move(result);
}

} // namespace capnproto
} // namespace message
} // namespace beam

#endif
