#ifndef BEAM_MESSAGE_BUFFER_POOL_HPP
#define BEAM_MESSAGE_BUFFER_POOL_HPP

#include <cstdint>
#include <functional>
#include <tuple>
#include <vector>
#include <beam/message/buffer.hpp>
#include <turbo/container/mpmc_ring_queue.hpp>
#include <turbo/toolset/attribute.hpp>

namespace beam {
namespace message {

typedef std::unique_ptr<buffer, std::function<void (buffer*)>> unique_pool_ptr;

class TURBO_SYMBOL_DECL buffer_pool
{
public:
    typedef std::uint32_t capacity_type;
    buffer_pool(std::size_t message_word_length, capacity_type capacity);
    inline const buffer& operator[](capacity_type reservation) const
    {
	return pool_[reservation];
    }
    inline buffer& operator[](capacity_type reservation)
    {
	return pool_[reservation];
    }
    capacity_type reserve();
    void revoke(capacity_type reservation);
    unique_pool_ptr borrow(std::size_t required_word_length);
    unique_pool_ptr borrow();
    unique_pool_ptr borrow_and_copy(kj::ArrayPtr<capnp::word> source);
private:
    typedef turbo::container::mpmc_ring_queue<capacity_type> free_list_type;
    buffer_pool() = delete;
    buffer_pool(const buffer_pool&) = delete;
    buffer_pool(buffer_pool&&) = delete;
    buffer_pool& operator=(const buffer_pool&) = delete;
    buffer_pool& operator=(buffer_pool&&) = delete;
    void reinstate(buffer* ptr);
    std::vector<buffer> pool_;
    free_list_type free_list_;
    std::size_t default_word_length_;
};

} // namespace message
} // namespace beam

#endif
