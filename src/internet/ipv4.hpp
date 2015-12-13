#ifndef BEAM_INTERNET_IP4_HPP
#define BEAM_INTERNET_IP4_HPP

#include <cstdint>
#include <array>
#include <string>
#include <vector>

namespace beam {
namespace internet {
namespace ipv4 {

typedef uint32_t address;

std::vector<address>&& resolve(const std::string& hostname);

} // namespace ipv4
} // namespace internet
} // namespace beam

#endif
