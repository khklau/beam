#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <tuple>
#include <asio/io_service.hpp>
#include <beam/queue/unordered_mixed.hpp>
#include <beam/queue/unordered_mixed.hxx>
#include <beam/queue/unordered_mixed_test.capnp.h>

namespace bii4 = beam::internet::ipv4;
namespace bqu = beam::queue::unordered_mixed;

TEST(unordered_mixed_test, basic)
{
    std::size_t disconnect_count = 0;
    asio::io_service service;
    bqu::sender<bqu::UnreliableMsg, bqu::ReliableMsg> sender1(service,
    {
	[&] ()
	{
	},
	[&] (const bii4::address&)
	{
	    ++disconnect_count;
	}
    },
    { 16U });
    sender1.connect(bii4::resolve("localhost"), 4088U);
    sender1.disconnect();
}
