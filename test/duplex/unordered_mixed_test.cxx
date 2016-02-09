#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <asio/io_service.hpp>
#include <beam/message/capnproto.hpp>
#include <beam/message/capnproto.hxx>
#include <beam/duplex/unordered_mixed.hpp>
#include <beam/duplex/unordered_mixed.hxx>
#include <beam/duplex/unordered_mixed_test.capnp.h>
#include <turbo/container/spsc_ring_queue.hpp>
#include <turbo/container/spsc_ring_queue.hxx>

namespace bii4 = beam::internet::ipv4;
namespace bme = beam::message;
namespace bdc = beam::duplex::common;
namespace bdu = beam::duplex::unordered_mixed;

namespace {

static const uint32_t localhost = 16777343U;

/*
class initiator_slave
{
public:
    typedef bdu::initiator<bdu::UnreliableMsg, bdu::ReliableMsg> initiator_type;
    initiator_slave(bii4::address address, bdc::port port, const initiator_type::perf_params& params);
    ~initiator_slave();
    inline bool is_running() const { return thread_ != nullptr; }
    inline bii4::address get_address() const { return address_; }
    inline bdc::port get_port() const { return port_; }
    void start();
    void stop();
    void send_unreliable(std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message);
    void send_reliable(std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message);
private:
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>>> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<bme::capnproto<bdu::ReliableMsg>>> reliable_queue_type;
    initiator_slave(const initiator_slave& other) = delete;
    initiator_slave& operator=(const initiator_slave& other) = delete;
    void run();
    void brake();
    void on_send_unreliable();
    void on_send_reliable();
    void on_disconnect(const bii4::address&, const beam::duplex::common::port&);
    bii4::address address_;
    bdc::port port_;
    std::thread* thread_;
    asio::io_service service_;
    asio::io_service::strand strand_;
    initiator_type initiator_;
    unreliable_queue_type unreliable_queue_;
    reliable_queue_type reliable_queue_;
};

struct receiver_master
{
    typedef bdu::receiver<bdu::UnreliableMsg, bdu::ReliableMsg> receiver_type;
    receiver_master(bdc::endpoint_id&& point, receiver_type::perf_params&& params);
    ~receiver_master();
    void bind(bdc::endpoint_id&& point);
    asio::io_service service;
    asio::io_service::strand strand;
    receiver_type receiver;
};

initiator_slave::initiator_slave(bii4::address address, bdc::port port, const initiator_type::perf_params& params) :
	address_(address),
	port_(port),
	thread_(nullptr),
	service_(),
	strand_(service_),
	initiator_(strand_, {std::bind(&initiator_slave::on_disconnect, this, std::placeholders::_1, std::placeholders::_2)}, params),
	unreliable_queue_(128),
	reliable_queue_(128)
{ }

initiator_slave::~initiator_slave()
{
    if (initiator_.is_connected() || !service_.stopped())
    {
	stop();
    }
    if (is_running())
    {
	thread_->join();
	delete thread_;
	thread_ = nullptr;
    }
}

void initiator_slave::start()
{
    if (!is_running())
    {
	std::function<void ()> entry(std::bind(&initiator_slave::run, this));
	thread_ = new std::thread(entry);
    }
}

void initiator_slave::stop()
{
    service_.post(std::bind(&initiator_slave::brake, this));
}

void initiator_slave::send_unreliable(std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message)
{
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_queue_.get_producer().try_enqueue_move(std::move(message))) << "Unreliable message enqueue failed";
    service_.post(std::bind(&initiator_slave::on_send_unreliable, this));
}

void initiator_slave::send_reliable(std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message)
{
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_queue_.get_producer().try_enqueue_move(std::move(message))) << "Reliable message enqueue failed";
    service_.post(std::bind(&initiator_slave::on_send_reliable, this));
}

void initiator_slave::run()
{
    ASSERT_EQ(initiator_type::connection_result::success, initiator_.connect({address_}, port_)) << "Connection failed";
    service_.run();
}

void initiator_slave::brake()
{
    if (initiator_.is_connected())
    {
	initiator_.disconnect();
    }
    service_.stop();
}

void initiator_slave::on_send_unreliable()
{
    std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message;
    ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_queue_.get_consumer().try_dequeue_move(message)) << "Unreliable message dequeue failed";
    initiator_.send_unreliable(*message);
}

void initiator_slave::on_send_reliable()
{
    std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message;
    ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_queue_.get_consumer().try_dequeue_move(message)) << "Reliable message dequeue failed";
    initiator_.send_reliable(*message);
}

void initiator_slave::on_disconnect(const bii4::address&, const beam::duplex::common::port&)
{
    GTEST_FATAL_FAILURE_("Unexpected disconnect");
}

receiver_master::receiver_master(bdc::endpoint_id&& point, receiver_type::perf_params&& params) :
	service(),
	strand(service),
	receiver(strand, std::move(params))
{
    bind(std::move(point));
}

receiver_master::~receiver_master()
{
    if (!service.stopped())
    {
	service.stop();
    }
}

void receiver_master::bind(bdc::endpoint_id&& point)
{
    ASSERT_EQ(receiver_type::bind_result::success, receiver.bind(point)) << "Bind failed";
}

void setupConnection(::receiver_master& master, ::initiator_slave& slave)
{
    std::size_t connection_count = 0;
    while (connection_count == 0)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		slave.start();
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address& address, const beam::duplex::common::port&)
	    {
		ASSERT_EQ(slave.get_address(), address) << "Connection on unexpected address";
		++connection_count;
	    },
	    [&](const bii4::address&, const beam::duplex::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>>)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](std::unique_ptr<bme::capnproto<bdu::ReliableMsg>>)
	    {
		GTEST_FATAL_FAILURE_("Unexpected reliable message");
	    }
	});
	master.service.run();
    }
    master.service.reset();
}

*/

} // anonymous namespace

TEST(unordered_mixed_test, basic_unreliable)
{
}

TEST(unordered_mixed_test, basic_reliable)
{
}
