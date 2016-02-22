#include "common.hpp"

namespace beam {
namespace duplex {
namespace common {

bool operator==(const endpoint_id& lhs, const endpoint_id& rhs)
{
    return lhs.address == rhs.address && lhs.port == rhs.port;
}

} // namespace common
} // namespace duplex
} // namespace beam
