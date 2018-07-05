#include "endpoint.hpp"
#include <limits>
#include <ostream>

namespace beam {
namespace internet {

endpoint_id::endpoint_id()
    :
	value_(0U)
{ }

endpoint_id::endpoint_id(ipv4::address addr, ipv4::port pt)
    :
	value_(static_cast<std::uint64_t>(addr) << 32U | static_cast<std::uint64_t>(pt))
{ }

ipv4::address endpoint_id::get_address() const
{
    return static_cast<ipv4::address>(value_ >> 32U);
}

ipv4::port endpoint_id::get_port() const
{
    return static_cast<ipv4::port>(value_ & std::numeric_limits<std::uint32_t>::max());
}

} // namespace internet
} // namespace beam

namespace std {

ostream& operator<<(ostream& stream, const beam::internet::endpoint_id& id)
{
    stream << "beam::internet::endpoint_id{address: " << id.get_address() << ", port: " << id.get_port() << "}";
    return stream;
}

} // namespace std
