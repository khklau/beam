#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <asio/io_service.hpp>
#include <beam/message/capnproto.hpp>
#include <beam/message/capnproto.hxx>
#include <beam/queue/unordered_mixed.hpp>
#include <beam/queue/unordered_mixed.hxx>
#include <beam/queue/unordered_mixed_test.capnp.h>
#include <turbo/container/spsc_ring_queue.hpp>
#include <turbo/container/spsc_ring_queue.hxx>

namespace bii4 = beam::internet::ipv4;
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
    void send_unreliable(std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message);
    void send_reliable(std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message);
private:
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>>> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<bme::capnproto<bqu::ReliableMsg>>> reliable_queue_type;
    sender_slave(const sender_slave& other) = delete;
    sender_slave& operator=(const sender_slave& other) = delete;
    void run();
    void brake();
    void on_send_unreliable();
    void on_send_reliable();
    void on_disconnect();
    bii4::address address_;
    bqc::port port_;
    std::thread* thread_;
    asio::io_service service_;
    sender_type sender_;
    unreliable_queue_type unreliable_queue_;
    reliable_queue_type reliable_queue_;
};

struct receiver_master
{
    typedef bqu::receiver<bqu::UnreliableMsg, bqu::ReliableMsg> receiver_type;
    receiver_master(bqc::endpoint&& point, receiver_type::perf_params&& params);
    ~receiver_master();
    void bind(bqc::endpoint&& point);
    asio::io_service service;
    receiver_type receiver;
};

sender_slave::sender_slave(bii4::address address, bqc::port port, const sender_type::perf_params& params) :
	address_(address),
	port_(port),
	thread_(nullptr),
	service_(),
	sender_(service_, {std::bind(&sender_slave::on_disconnect, this)}, params),
	unreliable_queue_(128),
	reliable_queue_(128)
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

void sender_slave::send_unreliable(std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message)
{
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_queue_.get_producer().try_enqueue_move(std::move(message))) << "Unreliable message enqueue failed";
    service_.post(std::bind(&sender_slave::on_send_unreliable, this));
}

void sender_slave::send_reliable(std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message)
{
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_queue_.get_producer().try_enqueue_move(std::move(message))) << "Reliable message enqueue failed";
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
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message;
    ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_queue_.get_consumer().try_dequeue_move(message)) << "Unreliable message dequeue failed";
    sender_.send_unreliable(*message);
}

void sender_slave::on_send_reliable()
{
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message;
    ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_queue_.get_consumer().try_dequeue_move(message)) << "Reliable message dequeue failed";
    sender_.send_reliable(*message);
}

void sender_slave::on_disconnect()
{
    GTEST_FATAL_FAILURE_("Unexpected disconnect");
}

receiver_master::receiver_master(bqc::endpoint&& point, receiver_type::perf_params&& params) :
	service(),
	receiver(service, std::move(params))
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

void receiver_master::bind(bqc::endpoint&& point)
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&&)
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
    ::receiver_master master({0U, 8888U}, {24U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8888U, {128U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::size_t unreliable_count = 0;
    while (unreliable_count == 0)
    {
	master.receiver.async_receive(
	{
	    [&](const ::receiver_master::receiver_type::event_handlers& current)
	    {
		std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message(new bme::capnproto<bqu::UnreliableMsg>());
		bqu::UnreliableMsg::Builder builder = message->get_builder();
		builder.setValue(123U);
		slave.send_unreliable(std::move(message));
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&& message)
	    {
		ASSERT_EQ(123U, message.get_reader().getValue()) << "Incorrect message value";
		++unreliable_count;
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&&)
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
    ::receiver_master master({0U, 8889U}, {24U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8889U, {128U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder = message->get_builder();
    builder.setValue("foo");
    slave.send_reliable(std::move(message));
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&& message)
	    {
		ASSERT_STREQ("foo", message.get_reader().getValue().cStr()) << "Incorrect message value";
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
    ::receiver_master master({0U, 8888U}, {24U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8888U, {128U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<uint32_t> values({123U, 456U, 789U});
    auto iter = values.begin();
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message1(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder1 = message1->get_builder();
    builder1.setValue(*iter);
    slave.send_unreliable(std::move(message1));
    ++iter;
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message2(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder2 = message2->get_builder();
    builder2.setValue(*iter);
    slave.send_unreliable(std::move(message2));
    ++iter;
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message3(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder3 = message3->get_builder();
    builder3.setValue(*iter);
    slave.send_unreliable(std::move(message3));
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&& message)
	    {
		auto result = values.find(message.get_reader().getValue());
		ASSERT_NE(values.end(), result) << "Incorrect message value";
		values.erase(result);
		++unreliable_count;
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&&)
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
    ::receiver_master master({0U, 8889U}, {24U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8889U, {128U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<std::string> values({"abc", "xyz", "!@#"});
    auto iter = values.begin();
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message1(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder1 = message1->get_builder();
    builder1.setValue(*iter);
    slave.send_reliable(std::move(message1));
    ++iter;
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message2(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder2 = message2->get_builder();
    builder2.setValue(*iter);
    slave.send_reliable(std::move(message2));
    ++iter;
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message3(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder3 = message3->get_builder();
    builder3.setValue(*iter);
    slave.send_reliable(std::move(message3));
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&& message)
	    {
		auto result = values.find(message.get_reader().getValue());
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
    ::receiver_master master({0U, 8890U}, {24U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8890U, {128U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message1(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder1 = message1->get_builder();
    builder1.setValue("bar");
    slave.send_reliable(std::move(message1));
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message2(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder2 = message2->get_builder();
    builder2.setValue(999U);
    slave.send_unreliable(std::move(message2));
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&& message)
	    {
		ASSERT_EQ(999U, message.get_reader().getValue()) << "Incorrect unreliable message value";
		++unreliable_count;
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&& message)
	    {
		ASSERT_STREQ("bar", message.get_reader().getValue().cStr()) << "Incorrect reliable message value";
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
    ::receiver_master master({0U, 8890U}, {24U, std::chrono::milliseconds(0)});
    ::sender_slave slave(::localhost, 8890U, {128U, std::chrono::microseconds(0)});
    setupConnection(master, slave);
    std::unordered_set<std::string> reliableValues({"abc", "xyz", "!@#"});
    auto reliableIter = reliableValues.begin();
    std::unordered_set<uint32_t> unreliableValues({123U, 456U, 789U});
    auto unreliableIter = unreliableValues.begin();
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message1(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder1 = message1->get_builder();
    builder1.setValue(*reliableIter);
    slave.send_reliable(std::move(message1));
    ++reliableIter;
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message2(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder2 = message2->get_builder();
    builder2.setValue(*unreliableIter);
    slave.send_unreliable(std::move(message2));
    ++unreliableIter;
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message3(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder3 = message3->get_builder();
    builder3.setValue(*unreliableIter);
    slave.send_unreliable(std::move(message3));
    ++unreliableIter;
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message4(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder4 = message4->get_builder();
    builder4.setValue(*reliableIter);
    slave.send_reliable(std::move(message4));
    ++reliableIter;
    std::unique_ptr<bme::capnproto<bqu::ReliableMsg>> message5(new bme::capnproto<bqu::ReliableMsg>());
    bqu::ReliableMsg::Builder builder5 = message5->get_builder();
    builder5.setValue(*reliableIter);
    slave.send_reliable(std::move(message5));
    ++reliableIter;
    std::unique_ptr<bme::capnproto<bqu::UnreliableMsg>> message6(new bme::capnproto<bqu::UnreliableMsg>());
    bqu::UnreliableMsg::Builder builder6 = message6->get_builder();
    builder6.setValue(*unreliableIter);
    slave.send_unreliable(std::move(message6));
    ++unreliableIter;
    std::size_t unreliable_count = 0;
    std::size_t reliable_count = 0;
    std::size_t reliable_target = reliableValues.size();
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
	    [&](bme::capnproto<bqu::UnreliableMsg>&& message)
	    {
		auto result = unreliableValues.find(message.get_reader().getValue());
		ASSERT_NE(unreliableValues.end(), result) << "Incorrect unreliable message value";
		unreliableValues.erase(result);
		++unreliable_count;
	    },
	    [&](bme::capnproto<bqu::ReliableMsg>&& message)
	    {
		auto result = reliableValues.find(message.get_reader().getValue());
		ASSERT_NE(reliableValues.end(), result) << "Incorrect reliable message value";
		reliableValues.erase(result);
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}
