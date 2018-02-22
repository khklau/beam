#include "buffer_pool.hpp"
#include <utility>
#include <turbo/container/mpmc_ring_queue.hxx>

namespace {

} // anonymous namespace

namespace beam {
namespace message {

buffer_pool::buffer_pool(std::size_t message_word_length, capacity_type capacity)
    :
	pool_(),
	free_list_(capacity),
	default_word_length_(message_word_length)
{
    pool_.reserve(capacity);
    for (std::size_t iter = 0U; iter < capacity; ++iter)
    {
	if (default_word_length_ > 0U)
	{
	    pool_[iter] = std::move(make_buffer(default_word_length_));
	}
	free_list_.try_enqueue_copy(iter);
    }
}

buffer_pool::capacity_type buffer_pool::reserve()
{
    namespace tar = turbo::algorithm::recovery;
    capacity_type reservation = 0U;
    tar::retry_with_random_backoff([&] () -> tar::try_state
    {
	switch (free_list_.try_dequeue_copy(reservation))
	{
	    case free_list_type::consumer::result::success:
	    {
		return tar::try_state::done;
	    }
	    default:
	    {
		return tar::try_state::retry;
	    }
	}
    });
    return reservation;
}

void buffer_pool::revoke(capacity_type reservation)
{
    namespace tar = turbo::algorithm::recovery;
    tar::retry_with_random_backoff([&] () -> tar::try_state
    {
	switch (free_list_.try_enqueue_copy(reservation))
	{
	    case free_list_type::producer::result::success:
	    {
		return tar::try_state::done;
	    }
	    default:
	    {
		return tar::try_state::retry;
	    }
	}
    });
}

unique_pool_ptr buffer_pool::borrow(std::size_t required_word_length)
{
    capacity_type reservation = reserve();
    if (pool_[reservation].size() < required_word_length)
    {
	std::size_t new_word_length = pool_[reservation].size();
	if (new_word_length == 0U)
	{
	    new_word_length = required_word_length;
	}
	else
	{
	    while (new_word_length < required_word_length)
	    {
		// FIXME: it's unlikely but we really should handle a potential overflow
		new_word_length *= 2U;
	    }
	}
	pool_[reservation] = std::move(make_buffer(new_word_length));
    }
    unique_pool_ptr result(
	    &(pool_[reservation]),
	    std::bind(&buffer_pool::reinstate, this, std::placeholders::_1));
    return std::move(result);
}

unique_pool_ptr buffer_pool::borrow()
{
    return std::move(borrow(default_word_length_));
}

unique_pool_ptr buffer_pool::borrow_and_copy(kj::ArrayPtr<capnp::word> source)
{
    auto buffer = std::move(borrow(source.size()));
    std::memcpy(buffer->begin(), source.begin(), sizeof(capnp::word) * buffer->size());
    return std::move(buffer);
}

void buffer_pool::reinstate(buffer* ptr)
{
    capacity_type reservation = &(pool_[0]) - ptr;
    revoke(reservation);
}

} // namespace message
} // namespace beam
