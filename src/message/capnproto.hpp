#ifndef BEAM_MESSAGE_CAPNPROTO_HPP
#define BEAM_MESSAGE_CAPNPROTO_HPP

#include <beam/message/buffer.hpp>
#include <capnp/common.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/array.h>
#include <kj/io.h>

namespace beam {
namespace message {

template <class message_t>
class capnproto
{
public:
    typedef message_t message_type;
    capnproto();
    explicit capnproto(kj::InputStream& input);
    explicit capnproto(kj::ArrayPtr<capnp::word> flat);
    capnproto(const capnproto<message_t>&) = delete;
    capnproto<message_t>& operator=(const capnproto<message_t>&) = delete;
    typename message_t::Builder get_builder();
    typename message_t::Reader get_reader();
    kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> get_segments();
private:
    capnp::MallocMessageBuilder message_;
};

template <class message_t>
class outbound
{
public:
    typedef message_t message_type;
    explicit outbound(buffer& storage);
    inline typename message_type::Builder build()
    {
	return builder_.initRoot<message_type>();
    }
private:
    outbound() = delete;
    capnp::MallocMessageBuilder builder_;
};

template <class message_t>
class inbound
{
public:
    typedef message_t message_type;
    inbound(buffer& storage, const kj::ArrayPtr<capnp::word> source);
    inline typename message_type::Reader read()
    {
	return builder_.getRoot<message_type>().asReader();
    }
private:
    inbound() = delete;
    capnp::MallocMessageBuilder builder_;
};

} // namespace message
} // namespace beam

#endif
