#ifndef BEAM_QUEUE_COMMON_HPP
#define BEAM_QUEUE_COMMON_HPP

#include <functional>

namespace beam {
namespace queue {
namespace common {

typedef uint16_t port;

struct endpoint_id
{
    beam::internet::ipv4::address address;
    beam::queue::common::port port;
};

} // namespace common
} // namespace queue
} // namespace beam

namespace std {

template<>
struct hash<beam::queue::common::endpoint_id>
{
    std::size_t operator()(beam::queue::common::endpoint_id value) const
    {
	uint64_t tmp = value.address;
	tmp = (tmp << 32) + value.port;
	std::hash<uint64_t> hasher;
	return hasher(tmp);
    }
};

} // namespace std

#endif
