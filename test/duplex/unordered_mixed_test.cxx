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

class initiator_slave
{
public:
    typedef bdu::in_connection<bdu::UnreliableMsg, bdu::ReliableMsg> in_connection_type;
    typedef bdu::out_connection<bdu::UnreliableMsg, bdu::ReliableMsg> out_connection_type;
    typedef bdu::initiator<in_connection_type, out_connection_type> initiator_type;
    initiator_slave(bdc::endpoint_id id, bdu::perf_params&& params);
    ~initiator_slave();
    inline bool is_running() const { return thread_ != nullptr; }
    inline const bdc::endpoint_id& get_endpoint_id() const { return id_; }
    void start();
    void stop();
    void send_unreliable(std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message);
    void send_reliable(std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message);
private:
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>>> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<bme::capnproto<bdu::ReliableMsg>>> reliable_queue_type;
    initiator_slave() = delete;
    initiator_slave(const initiator_slave& other) = delete;
    initiator_slave& operator=(const initiator_slave& other) = delete;
    void run();
    void brake();
    void on_send_unreliable(out_connection_type& connection);
    void on_send_reliable(out_connection_type& connection);
    void on_disconnect(const bii4::address&, const beam::duplex::common::port&);
    bdc::endpoint_id id_;
    std::thread* thread_;
    asio::io_service service_;
    asio::io_service::strand strand_;
    initiator_type initiator_;
    unreliable_queue_type unreliable_queue_;
    reliable_queue_type reliable_queue_;
    in_connection_type::event_handlers handlers_;
};

initiator_slave::initiator_slave(bdc::endpoint_id id, bdu::perf_params&& params) :
	id_(id),
	thread_(nullptr),
	service_(),
	strand_(service_),
	initiator_(strand_, std::move(params)),
	unreliable_queue_(128),
	reliable_queue_(128),
	handlers_(
	{
	    [&](const in_connection_type::event_handlers&)
	    {
		initiator_.async_receive(handlers_);
	    },
	    [&](const in_connection_type&)
	    {
		initiator_.async_receive(handlers_);
	    },
	    [&](const in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](const in_connection_type&, std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>>)
	    {
		initiator_.async_receive(handlers_);
	    },
	    [&](const in_connection_type&, std::unique_ptr<bme::capnproto<bdu::ReliableMsg>>)
	    {
		initiator_.async_receive(handlers_);
	    }
	})
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
    initiator_.async_send(std::bind(&initiator_slave::on_send_unreliable, this, std::placeholders::_1));
}

void initiator_slave::send_reliable(std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message)
{
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_queue_.get_producer().try_enqueue_move(std::move(message))) << "Reliable message enqueue failed";
    initiator_.async_send(std::bind(&initiator_slave::on_send_reliable, this, std::placeholders::_1));
}

void initiator_slave::run()
{
    ASSERT_EQ(bdu::connection_result::success, initiator_.connect({id_.address}, id_.port)) << "Connection failed";
    initiator_.async_receive(handlers_);
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

void initiator_slave::on_send_unreliable(out_connection_type& connection)
{
    std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message;
    ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_queue_.get_consumer().try_dequeue_move(message)) << "Unreliable message dequeue failed";
    connection.send_unreliable(*message);
}

void initiator_slave::on_send_reliable(out_connection_type& connection)
{
    std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message;
    ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_queue_.get_consumer().try_dequeue_move(message)) << "Reliable message dequeue failed";
    connection.send_reliable(*message);
}

void initiator_slave::on_disconnect(const bii4::address&, const beam::duplex::common::port&)
{
    GTEST_FATAL_FAILURE_("Unexpected disconnect");
}

struct responder_master
{
    typedef bdu::in_connection<bdu::UnreliableMsg, bdu::ReliableMsg> in_connection_type;
    typedef bdu::out_connection<bdu::UnreliableMsg, bdu::ReliableMsg> out_connection_type;
    typedef bdu::responder<in_connection_type, out_connection_type> responder_type;
    responder_master(bdc::endpoint_id&& point, bdu::perf_params&& params);
    ~responder_master();
    void bind(bdc::endpoint_id&& point);
    asio::io_service service;
    asio::io_service::strand strand;
    responder_type responder;
};

responder_master::responder_master(bdc::endpoint_id&& point, bdu::perf_params&& params) :
	service(),
	strand(service),
	responder(strand, std::move(params))
{
    bind(std::move(point));
}

responder_master::~responder_master()
{
    if (!service.stopped())
    {
	service.stop();
    }
}

void responder_master::bind(bdc::endpoint_id&& point)
{
    ASSERT_EQ(bdu::bind_result::success, responder.bind(point)) << "Bind failed";
}

void setupConnection(::responder_master& master, ::initiator_slave& slave)
{
    std::size_t connection_count = 0;
    while (connection_count == 0)
    {
	master.responder.async_receive(
	{
	    [&](const ::responder_master::in_connection_type::event_handlers& current)
	    {
		slave.start();
		master.responder.async_receive(current);
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		++connection_count;
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](const ::responder_master::in_connection_type&, std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>>)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](const ::responder_master::in_connection_type&, std::unique_ptr<bme::capnproto<bdu::ReliableMsg>>)
	    {
		GTEST_FATAL_FAILURE_("Unexpected reliable message");
	    }
	});
	master.service.run();
    }
    master.service.reset();
}

} // anonymous namespace

TEST(unordered_mixed_test, basic_unreliable)
{
    ::responder_master master({0U, 8888U}, {24U});
    ::initiator_slave slave({::localhost, 8888U}, {24U});
    setupConnection(master, slave);
    std::size_t unreliable_count = 0;
    while (unreliable_count == 0)
    {
	master.responder.async_receive(
	{
	    [&](const ::responder_master::in_connection_type::event_handlers& current)
	    {
		std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message(new bme::capnproto<bdu::UnreliableMsg>());
		bdu::UnreliableMsg::Builder builder = message->get_builder();
		builder.setValue(123U);
		slave.send_unreliable(std::move(message));
		master.responder.async_receive(current);
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](const ::responder_master::in_connection_type&, std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>> message)
	    {
		ASSERT_EQ(123U, message->get_reader().getValue()) << "Incorrect message value";
		++unreliable_count;
	    },
	    [&](const ::responder_master::in_connection_type&, std::unique_ptr<bme::capnproto<bdu::ReliableMsg>>)
	    {
		GTEST_FATAL_FAILURE_("Unexpected reliable message");
	    }
	});
	master.service.run();
    };
    slave.stop();
}

TEST(unordered_mixed_test, basic_reliable)
{
    ::responder_master master({0U, 8888U}, {24U});
    ::initiator_slave slave({::localhost, 8888U}, {24U});
    setupConnection(master, slave);
    std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message(new bme::capnproto<bdu::ReliableMsg>());
    bdu::ReliableMsg::Builder builder = message->get_builder();
    builder.setValue("foo");
    slave.send_reliable(std::move(message));
    std::size_t reliable_count = 0;
    while (reliable_count == 0)
    {
	master.responder.async_receive(
	{
	    [&](const ::responder_master::in_connection_type::event_handlers& current)
	    {
		master.responder.async_receive(current);
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](const ::responder_master::in_connection_type&, std::unique_ptr<bme::capnproto<bdu::UnreliableMsg>>)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](const ::responder_master::in_connection_type&, std::unique_ptr<bme::capnproto<bdu::ReliableMsg>> message)
	    {
		ASSERT_STREQ("foo", message->get_reader().getValue().cStr()) << "Incorrect message value";
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();

}
