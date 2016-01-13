#ifndef BEAM_MESSAGE_CAPNPROTO_HPP
#define BEAM_MESSAGE_CAPNPROTO_HPP

#include <capnp/common.h>
#include <capnp/message.h>
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
    explicit capnproto(kj::Array<capnp::word>& input);
    capnproto(const capnproto<message_t>&) = delete;
    capnproto<message_t>& operator=(const capnproto<message_t>&) = delete;
    typename message_t::Builder getBuilder();
    typename message_t::Reader getReader();
    kj::Array<capnp::word> toFlatArray();
private:
    capnp::MallocMessageBuilder message_;
};

} // namespace message
} // namespace beam

#endif
