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

unique_pool_ptr buffer_pool::borrow()
{
    namespace tar = turbo::algorithm::recovery;
    capacity_type reservation = 0U;
    unique_pool_ptr result(nullptr, std::bind(&buffer_pool::release, this, std::placeholders::_1));
    tar::retry_with_random_backoff([&] () -> tar::try_state
    {
	switch (free_list_.try_dequeue_copy(reservation))
	{
	    case free_list_type::consumer::result::success:
	    {
		result.reset(&(pool_[reservation]));
		return tar::try_state::done;
	    }
	    default:
	    {
		return tar::try_state::retry;
	    }
	}
    });
    return std::move(result);
}

void buffer_pool::release(buffer* ptr)
{
    namespace tar = turbo::algorithm::recovery;
    capacity_type index = &(pool_[0]) - ptr;
    tar::retry_with_random_backoff([&] () -> tar::try_state
    {
	switch (free_list_.try_enqueue_copy(index))
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

} // namespace message
} // namespace beam
