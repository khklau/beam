#ifndef BEAM_MESSAGE_CAPNPROTO_HPP
#define BEAM_MESSAGE_CAPNPROTO_HPP

#include <utility>
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
    explicit capnproto(unique_pool_ptr&& buffer);
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

} // namespace message
} // namespace beam

#endif
