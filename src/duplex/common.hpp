#ifndef BEAM_DUPLEX_COMMON_HPP
#define BEAM_DUPLEX_COMMON_HPP

#include <functional>
#include <beam/internet/ipv4.hpp>

namespace beam {
namespace duplex {
namespace common {

typedef uint16_t port;

struct endpoint_id
{
    beam::internet::ipv4::address address;
    beam::duplex::common::port port;
};

bool operator==(const endpoint_id& lhs, const endpoint_id& rhs)
{
    return lhs.address == rhs.address && lhs.port == rhs.port;
}

} // namespace common
} // namespace duplex
} // namespace beam

namespace std {

template<>
struct hash<beam::duplex::common::endpoint_id>
{
    std::size_t operator()(beam::duplex::common::endpoint_id value) const
    {
	uint64_t tmp = value.address;
	tmp = (tmp << 32) + value.port;
	std::hash<uint64_t> hasher;
	return hasher(tmp);
    }
};

} // namespace std

#endif
