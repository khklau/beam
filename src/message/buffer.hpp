#ifndef BEAM_MESSAGE_BUFFER_HPP
#define BEAM_MESSAGE_BUFFER_HPP

#include <cstdint>
#include <cstring>
#include <algorithm>
#include <functional>
#include <memory>
#include <capnp/common.h>
#include <kj/array.h>

namespace beam {
namespace message {

typedef kj::Array<capnp::word> buffer;

inline buffer make_buffer(std::size_t word_length)
{
    buffer result = std::move(kj::heapArray<capnp::word>(word_length));
    // can't use std::fill_n because capnp::word isn't copyable
    std::memset(result.begin(), 0, sizeof(capnp::word) * word_length);
    return std::move(result);
}

} // namespace message
} // namespace beam

#endif
