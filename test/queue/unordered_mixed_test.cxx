#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <asio/io_service.hpp>
#include <beam/message/buffer_pool.hpp>
#include <beam/message/capnproto.hpp>
#include <beam/message/capnproto.hxx>
#include <beam/queue/unordered_mixed.hpp>
#include <beam/queue/unordered_mixed.hxx>
#include <beam/queue/unordered_mixed_test.capnp.h>
#include <turbo/container/spsc_ring_queue.hpp>
#include <turbo/container/spsc_ring_queue.hxx>

namespace bii4 = beam::internet::ipv4;
namespace bmc = beam::message::capnproto;
namespace bme = beam::message;
namespace bqc = beam::queue::common;
namespace bqu = beam::queue::unordered_mixed;

namespace {

static const uint32_t localhost = 16777343U;

class sender_slave
{
public:
    typedef bqu::sender<bqu::UnreliableMsg, bqu::ReliableMsg> sender_type;
    sender_slave(bii4::address address, bqc::port port, const sender_type::perf_params& params);
    ~sender_slave();
    inline bool is_running() const { return thread_ != nullptr; }
    inline bii4::address get_address() const { return address_; }
    inline bqc::port get_port() const { return port_; }
    void start();
    void stop();
    void send_unreliable(bmc::form<bqu::UnreliableMsg>& message);
    void send_reliable(bmc::form<bqu::ReliableMsg>& message);
private:
    typedef turbo::container::spsc_ring_queue<bmc::payload<bqu::UnreliableMsg>> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<bmc::payload<bqu::ReliableMsg>> reliable_queue_type;
    sender_slave(const sender_slave& other) = delete;
    sender_slave& operator=(const sender_slave& other) = delete;
    void run();
    void brake();
    void on_send_unreliable();
    void on_send_reliable();
    void on_disconnect(const bii4::address&, const beam::queue::common::port&);
    bii4::address address_;
    bqc::port port_;
    std::thread* thread_;
    asio::io_service service_;
    asio::io_service::strand strand_;
    sender_type sender_;
    bme::buffer_pool pool_;
    unreliable_queue_type unreliable_queue_;
    reliable_queue_type reliable_queue_;
    unreliable_queue_type::producer& unreliable_producer_;
    unreliable_queue_type::consumer& unreliable_consumer_;
    reliable_queue_type::producer& reliable_producer_;
    reliable_queue_type::consumer& reliable_consumer_;
};

struct receiver_master
{
    typedef bqu::receiver<bqu::UnreliableMsg, bqu::ReliableMsg> receiver_type;
    receiver_master(bqc::endpoint_id&& point, receiver_type::perf_params&& params);
    ~receiver_master();
    void bind(bqc::endpoint_id&& point);
    asio::io_service service;
    asio::io_service::strand strand;
    bme::buffer_pool pool;
    receiver_type receiver;
};

sender_slave::sender_slave(bii4::address address, bqc::port port, const sender_type::perf_params& params) :
	address_(address),
	port_(port),
	thread_(nullptr),
	service_(),
	strand_(service_),
	sender_(strand_, {std::bind(&sender_slave::on_disconnect, this, std::placeholders::_1, std::placeholders::_2)}, params),
	pool_(8U, params.window_size),
	unreliable_queue_(128),
	reliable_queue_(128),
	unreliable_producer_(unreliable_queue_.get_producer()),
	unreliable_consumer_(unreliable_queue_.get_consumer()),
	reliable_producer_(reliable_queue_.get_producer()),
	reliable_consumer_(reliable_queue_.get_consumer())
{ }

sender_slave::~sender_slave()
{
    if (sender_.is_connected() || !service_.stopped())
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

void sender_slave::start()
{
    if (!is_running())
    {
	std::function<void ()> entry(std::bind(&sender_slave::run, this));
	thread_ = new std::thread(entry);
    }
}

void sender_slave::stop()
{
    service_.post(std::bind(&sender_slave::brake, this));
}

void sender_slave::send_unreliable(bmc::form<bqu::UnreliableMsg>& message)
{
    bmc::payload<bqu::UnreliableMsg> payload(std::move(bmc::serialise(pool_, message)));
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_producer_.try_enqueue_move(std::move(payload)))
	    << "Unreliable message enqueue failed";
    service_.post(std::bind(&sender_slave::on_send_unreliable, this));
}

void sender_slave::send_reliable(bmc::form<bqu::ReliableMsg>& message)
{
    bmc::payload<bqu::ReliableMsg> payload(std::move(bmc::serialise(pool_, message)));
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_producer_.try_enqueue_move(std::move(payload)))
	    << "Reliable message enqueue failed";
    service_.post(std::bind(&sender_slave::on_send_reliable, this));
}

void sender_slave::run()
{
    ASSERT_EQ(sender_type::connection_result::success, sender_.connect({address_}, port_)) << "Connection failed";
    service_.run();
}

void sender_slave::brake()
{
    if (sender_.is_connected())
    {
	sender_.disconnect();
    }
    service_.stop();
}

void sender_slave::on_send_unreliable()
{
    bmc::payload<bqu::UnreliableMsg> message;
    ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_consumer_.try_dequeue_move(message))
	    << "Unreliable message dequeue failed";
    sender_.send_unreliable(message);
}

void sender_slave::on_send_reliable()
{
    bmc::payload<bqu::ReliableMsg> message;
    ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_consumer_.try_dequeue_move(message))
	    << "Reliable message dequeue failed";
    sender_.send_reliable(message);
}

void sender_slave::on_disconnect(const bii4::address&, const beam::queue::common::port&)
{
    GTEST_FATAL_FAILURE_("Unexpected disconnect");
}

receiver_master::receiver_master(bqc::endpoint_id&& point, receiver_type::perf_params&& params) :
	service(),
	strand(service),
	pool(8U, params.window_size),
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

void receiver_master::bind(bqc::endpoint_id&& point)
{
    ASSERT_EQ(receiver_type::bind_result::success, receiver.bind(point)) << "Bind failed";
}

void setupConnection(::receiver_master& master, ::sender_slave& slave)
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
	    [&](const bii4::address& address, const beam::queue::common::port&)
	    {
		ASSERT_EQ(slave.get_address(), address) << "Connection on unexpected address";
		++connection_count;
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](bmc::statement<bqu::ReliableMsg>&)
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
    ::receiver_master master({0U, 8888U}, {24U, 64U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8888U, {64U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::size_t unreliable_count = 0;
    while (unreliable_count == 0)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		bmc::form<bqu::UnreliableMsg> message(std::move(master.pool.borrow()));
		bqu::UnreliableMsg::Builder builder = message.build();
		builder.setValue(123U);
		slave.send_unreliable(message);
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>& message)
	    {
		ASSERT_EQ(123U, message.read().getValue()) << "Incorrect message value";
		++unreliable_count;
	    },
	    [&](bmc::statement<bqu::ReliableMsg>&)
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
    ::receiver_master master({0U, 8889U}, {24U, 64U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8889U, {64U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    bmc::form<bqu::ReliableMsg> message(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder = message.build();
    builder.setValue("foo");
    slave.send_reliable(message);
    std::size_t reliable_count = 0;
    while (reliable_count == 0)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](bmc::statement<bqu::ReliableMsg>& message)
	    {
		ASSERT_STREQ("foo", message.read().getValue().cStr()) << "Incorrect message value";
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}

TEST(unordered_mixed_test, multi_unreliable)
{
    ::receiver_master master({0U, 8888U}, {24U, 64U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8888U, {64U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<uint32_t> values({123U, 456U, 789U});
    auto iter = values.begin();
    bmc::form<bqu::UnreliableMsg> message1(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder1 = message1.build();
    builder1.setValue(*iter);
    slave.send_unreliable(message1);
    ++iter;
    bmc::form<bqu::UnreliableMsg> message2(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder2 = message2.build();
    builder2.setValue(*iter);
    slave.send_unreliable(message2);
    ++iter;
    bmc::form<bqu::UnreliableMsg> message3(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder3 = message3.build();
    builder3.setValue(*iter);
    slave.send_unreliable(message3);
    ++iter;
    std::size_t unreliable_count = 0;
    while (unreliable_count == 0)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>& message)
	    {
		auto result = values.find(message.read().getValue());
		ASSERT_NE(values.end(), result) << "Incorrect message value";
		values.erase(result);
		++unreliable_count;
	    },
	    [&](bmc::statement<bqu::ReliableMsg>&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected reliable message");
	    }
	});
	master.service.run();
	master.service.reset();
    }
    slave.stop();
}

TEST(unordered_mixed_test, multi_reliable)
{
    ::receiver_master master({0U, 8889U}, {24U, 64U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8889U, {64U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<std::string> values({"abc", "xyz", "!@#"});
    auto iter = values.begin();
    bmc::form<bqu::ReliableMsg> message1(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder1 = message1.build();
    builder1.setValue(*iter);
    slave.send_reliable(message1);
    ++iter;
    bmc::form<bqu::ReliableMsg> message2(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder2 = message2.build();
    builder2.setValue(*iter);
    slave.send_reliable(message2);
    ++iter;
    bmc::form<bqu::ReliableMsg> message3(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder3 = message3.build();
    builder3.setValue(*iter);
    slave.send_reliable(message3);
    ++iter;
    std::size_t reliable_count = 0;
    std::size_t target_count = values.size();
    while (reliable_count < target_count)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](bmc::statement<bqu::ReliableMsg>& message)
	    {
		auto result = values.find(message.read().getValue());
		ASSERT_NE(values.end(), result) << "Incorrect message value";
		values.erase(result);
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}

TEST(unordered_mixed_test, basic_mixed)
{
    ::receiver_master master({0U, 8890U}, {24U, 64U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8890U, {64U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    bmc::form<bqu::ReliableMsg> message1(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder1 = message1.build();
    builder1.setValue("bar");
    slave.send_reliable(message1);
    bmc::form<bqu::UnreliableMsg> message2(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder2 = message2.build();
    builder2.setValue(999U);
    slave.send_unreliable(message2);
    std::size_t reliable_count = 0;
    std::size_t unreliable_count = 0;
    while (reliable_count == 0 || unreliable_count == 0)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>& message)
	    {
		ASSERT_EQ(999U, message.read().getValue()) << "Incorrect unreliable message value";
		++unreliable_count;
	    },
	    [&](bmc::statement<bqu::ReliableMsg>& message)
	    {
		ASSERT_STREQ("bar", message.read().getValue().cStr()) << "Incorrect reliable message value";
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}

TEST(unordered_mixed_test, multi_mixed)
{
    ::receiver_master master({0U, 8890U}, {24U, 64U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8890U, {64U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<std::string> reliable_values({"abc", "xyz", "!@#"});
    auto reliable_iter = reliable_values.begin();
    std::unordered_set<uint32_t> unreliable_values({123U, 456U, 789U});
    auto unreliable_iter = unreliable_values.begin();
    bmc::form<bqu::ReliableMsg> message1(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder1 = message1.build();
    builder1.setValue(*reliable_iter);
    slave.send_reliable(message1);
    ++reliable_iter;
    bmc::form<bqu::UnreliableMsg> message2(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder2 = message2.build();
    builder2.setValue(*unreliable_iter);
    slave.send_unreliable(message2);
    ++unreliable_iter;
    bmc::form<bqu::UnreliableMsg> message3(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder3 = message3.build();
    builder3.setValue(*unreliable_iter);
    slave.send_unreliable(message3);
    ++unreliable_iter;
    bmc::form<bqu::ReliableMsg> message4(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder4 = message4.build();
    builder4.setValue(*reliable_iter);
    slave.send_reliable(message4);
    ++reliable_iter;
    bmc::form<bqu::ReliableMsg> message5(std::move(master.pool.borrow()));
    bqu::ReliableMsg::Builder builder5 = message5.build();
    builder5.setValue(*reliable_iter);
    slave.send_reliable(message5);
    ++reliable_iter;
    bmc::form<bqu::UnreliableMsg> message6(std::move(master.pool.borrow()));
    bqu::UnreliableMsg::Builder builder6 = message6.build();
    builder6.setValue(*unreliable_iter);
    slave.send_unreliable(message6);
    ++unreliable_iter;
    std::size_t unreliable_count = 0;
    std::size_t reliable_count = 0;
    std::size_t reliable_target = reliable_values.size();
    while (unreliable_count == 0 || reliable_count != reliable_target)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>& message)
	    {
		auto result = unreliable_values.find(message.read().getValue());
		ASSERT_NE(unreliable_values.end(), result) << "Incorrect unreliable message value";
		unreliable_values.erase(result);
		++unreliable_count;
	    },
	    [&](bmc::statement<bqu::ReliableMsg>& message)
	    {
		auto result = reliable_values.find(message.read().getValue());
		ASSERT_NE(reliable_values.end(), result) << "Incorrect reliable message value";
		reliable_values.erase(result);
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}

TEST(unordered_mixed_test, recycled_buffers)
{
    ::receiver_master master({0U, 8890U}, {24U, 4U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8890U, {4U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<std::string> reliable_values({"abc", "xyz", "!@#", "*()"});
    auto reliable_iter = reliable_values.begin();
    std::unordered_set<uint32_t> unreliable_values({123U, 456U, 789U, 0U});
    auto unreliable_iter = unreliable_values.begin();
    std::size_t unreliable_count = 0;
    std::size_t unreliable_sent = 0;
    std::size_t reliable_count = 0;
    std::size_t reliable_sent = 0;
    std::size_t reliable_target = reliable_values.size();
    while (unreliable_count == 0 || reliable_count != reliable_target)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		if (reliable_iter != reliable_values.end() && reliable_sent <= reliable_count)
		{
		    bmc::form<bqu::ReliableMsg> message(std::move(master.pool.borrow()));
		    bqu::ReliableMsg::Builder builder = message.build();
		    builder.setValue(*reliable_iter);
		    slave.send_reliable(message);
		    ++reliable_sent;
		}
		if (unreliable_iter != unreliable_values.end() && unreliable_sent <= unreliable_count)
		{
		    bmc::form<bqu::UnreliableMsg> message(std::move(master.pool.borrow()));
		    bqu::UnreliableMsg::Builder builder = message.build();
		    builder.setValue(*unreliable_iter);
		    slave.send_unreliable(message);
		    ++unreliable_sent;
		}
		master.receiver.async_receive(current);
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected connect");
	    },
	    [&](const bii4::address&, const beam::queue::common::port&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](bmc::statement<bqu::UnreliableMsg>& message)
	    {
		auto result = unreliable_values.find(message.read().getValue());
		ASSERT_NE(unreliable_values.end(), result) << "Incorrect unreliable message value";
		++unreliable_iter;
		++unreliable_count;
	    },
	    [&](bmc::statement<bqu::ReliableMsg>& message)
	    {
		auto result = reliable_values.find(message.read().getValue());
		ASSERT_NE(reliable_values.end(), result) << "Incorrect reliable message value";
		++reliable_iter;
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}
