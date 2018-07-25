#ifndef BEAM_INTERNET_ENDPOINT_HPP
#define BEAM_INTERNET_ENDPOINT_HPP

#include <cstdint>
#include <iosfwd>
#include <beam/internet/ipv4.hpp>

namespace beam {
namespace internet {

class endpoint_id
{
public:
    endpoint_id();
    explicit endpoint_id(std::uint64_t value) : value_(value) { }
    endpoint_id(ipv4::address addr, ipv4::port pt);
    inline bool operator==(const endpoint_id& other) const
    {
	return this->value_ == other.value_;
    }
    inline bool operator!=(const endpoint_id& other) const
    {
	return !(*this == other);
    }
    inline bool operator<(const endpoint_id& other) const
    {
	return this->value_ < other.value_;
    }
    inline bool operator<=(const endpoint_id& other) const
    {
	return this->value_ <= other.value_;
    }
    inline bool operator>(const endpoint_id& other) const
    {
	return this->value_ > other.value_;
    }
    inline bool operator>=(const endpoint_id& other) const
    {
	return this->value_ >= other.value_;
    }
    ipv4::address get_address() const;
    ipv4::port get_port() const;
    inline std::uint64_t get_value() const { return value_; }
private:
    std::uint64_t value_;
};

} // namespace internet
} // namespace beam

namespace std {

ostream& operator<<(ostream& stream, const beam::internet::endpoint_id& id);

template<>
struct hash<beam::internet::endpoint_id>
{
    std::size_t operator()(beam::internet::endpoint_id id) const
    {
	std::hash<std::uint64_t> hasher;
	return hasher(id.get_value());
    }
};

} // namespace std

#endif
