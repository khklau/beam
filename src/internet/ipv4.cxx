#include "ipv4.hpp"
#include <utility>
#include <asio/ip/udp.hpp>

namespace beam {
namespace internet {
namespace ipv4 {

std::vector<address>&& resolve(const std::string& hostname)
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
