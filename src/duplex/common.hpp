#ifndef BEAM_DUPLEX_COMMON_HPP
#define BEAM_DUPLEX_COMMON_HPP

#include <functional>
#include <beam/internet/ipv4.hpp>

namespace beam {
namespace duplex {
namespace common {

typedef uint16_t port;

struct identity
{
    beam::internet::ipv4::address address;
    beam::duplex::common::port port;
};

} // namespace common
} // namespace duplex
} // namespace beam

namespace std {

template<>
struct hash<beam::duplex::common::identity>
{
    std::size_t operator()(beam::duplex::common::identity value) const
    {
	uint64_t tmp = value.address;
	tmp = (tmp << 32) + value.port;
	std::hash<uint64_t> hasher;
	return hasher(tmp);
    }
};

} // namespace std

#endif
