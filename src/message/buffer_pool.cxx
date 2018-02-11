#include "buffer_pool.hpp"
#include <utility>
#include <turbo/container/mpmc_ring_queue.hxx>

namespace {

} // anonymous namespace

namespace beam {
namespace message {

buffer_pool::buffer_pool(std::size_t message_size, capacity_type capacity)
    :
	pool_(),
	free_list_(capacity)
{
    for (auto iter = pool_.begin(); iter != pool_.end(); ++iter)
    {
	*iter = make_buffer(message_size);
	free_list_.try_enqueue_copy(pool_.begin() - iter);
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

unique_pool_ptr buffer_pool::borrow()
{
    capacity_type reservation = reserve();
    unique_pool_ptr result(
	    &(pool_[reservation]),
	    std::bind(&buffer_pool::reinstate, this, std::placeholders::_1));
    return std::move(result);
}

void buffer_pool::reinstate(buffer* ptr)
{
    capacity_type reservation = &(pool_[0]) - ptr;
    revoke(reservation);
}

} // namespace message
} // namespace beam
