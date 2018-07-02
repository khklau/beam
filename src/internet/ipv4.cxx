#include "ipv4.hpp"
#include <limits>
#include <ostream>
#include <utility>
#include <asio/ip/udp.hpp>

namespace beam {
namespace internet {
namespace ipv4 {

endpoint_id::endpoint_id()
    :
	value_(0U)
{ }

endpoint_id::endpoint_id(address addr, port pt)
    :
	value_(static_cast<std::uint64_t>(addr) << 32U | static_cast<std::uint64_t>(pt))
{ }

address endpoint_id::get_address() const
{
    return static_cast<address>(value_ >> 32U);
}

port endpoint_id::get_port() const
{
    return static_cast<port>(value_ & std::numeric_limits<std::uint32_t>::max());
}

std::vector<address> resolve(const std::string& hostname)
{
    std::vector<address> result;
    asio::io_service service;
    asio::ip::udp::resolver resolver(service);
    asio::ip::udp::resolver::query query(hostname);
    asio::ip::udp::resolver::iterator end;
    for (auto iter = resolver.resolve(query); iter != end; ++iter)
    {
	result.push_back(iter->endpoint().address().to_v4().to_ulong());
    }
    return std::move(result);
}

} // namespace ipv4
} // namespace internet
} // namespace beam

namespace std {

ostream& operator<<(ostream& stream, const beam::internet::ipv4::endpoint_id& id)
{
    stream << "beam::internet::ipv4::endpoint_id{address: " << id.get_address() << ", port: " << id.get_port() << "}";
    return stream;
}

} // namespace std
