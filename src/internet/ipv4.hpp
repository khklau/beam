#ifndef BEAM_INTERNET_IP4_HPP
#define BEAM_INTERNET_IP4_HPP

#include <cstdint>
#include <array>
#include <string>
#include <vector>

namespace beam {
namespace internet {
namespace ipv4 {

typedef std::uint32_t address;
typedef std::uint16_t port;

class endpoint_id
{
public:
    endpoint_id();
    endpoint_id(address addr, port pt);
    address get_address() const;
    port get_port() const;
    inline std::uint64_t get_value() const { return value_; }
private:
    std::uint64_t value_;
};

std::vector<address> resolve(const std::string& hostname);

} // namespace ipv4
} // namespace internet
} // namespace beam

#endif
