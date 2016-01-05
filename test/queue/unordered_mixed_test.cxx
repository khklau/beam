#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <asio/io_service.hpp>
#include <beam/queue/unordered_mixed.hpp>
#include <beam/queue/unordered_mixed.hxx>
#include <beam/queue/unordered_mixed_test.capnp.h>
#include <turbo/container/spsc_ring_queue.hpp>
#include <turbo/container/spsc_ring_queue.hxx>

namespace bii4 = beam::internet::ipv4;
namespace bqc = beam::queue::common;
namespace bqu = beam::queue::unordered_mixed;

class sender_slave
{
public:
    typedef bqu::sender<bqu::UnreliableMsg, bqu::ReliableMsg> sender_type;
    sender_slave(bii4::address address, bqc::port port, const sender_type::perf_params& params);
    ~sender_slave();
    inline bool is_running() const { return thread_ != nullptr; }
    void start();
    void stop();
    void send_reliable(std::unique_ptr<capnp::MallocMessageBuilder> message);
private:
    typedef turbo::container::spsc_ring_queue<std::unique_ptr<capnp::MallocMessageBuilder>> queue_type;
    sender_slave(const sender_slave& other) = delete;
    sender_slave& operator=(const sender_slave& other) = delete;
    void run();
    void on_send_reliable();
    void on_disconnect();
    bii4::address address_;
    bqc::port port_;
    std::thread* thread_;
    asio::io_service service_;
    sender_type sender_;
    queue_type queue_;
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
	queue_(128)
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
    if (sender_.is_connected())
    {
	sender_.disconnect();
    }
    service_.stop();
}

void sender_slave::send_reliable(std::unique_ptr<capnp::MallocMessageBuilder> message)
{
    ASSERT_EQ(queue_type::producer::result::success, queue_.get_producer().try_enqueue_move(std::move(message)));
    service_.post(std::bind(&sender_slave::on_send_reliable, this));
}

void sender_slave::run()
{
    ASSERT_EQ(sender_type::connection_result::success, sender_.connect({address_}, port_)) << "Connection failed";
    service_.run();
}

void sender_slave::on_send_reliable()
{
    std::unique_ptr<capnp::MallocMessageBuilder> message;
    ASSERT_EQ(queue_type::consumer::result::success, queue_.get_consumer().try_dequeue_move(message));
    sender_.send_reliable(*message);
}

void sender_slave::on_disconnect()
{
    ASSERT_FALSE(true) << "Unwanted disconnect";
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


TEST(unordered_mixed_test, basic)
{
    {
	std::size_t connection_count = 0;
	receiver_master master({0U, 8888U}, {24U, std::chrono::milliseconds(0)});
	sender_slave slave(16777343U, 8888U, {128U, std::chrono::microseconds(0)});
	while (connection_count == 0)
	{
	    master.receiver.async_receive(
	    {
		[&](const receiver_master::receiver_type::event_handlers& current)
		{
		    slave.start();
		    master.receiver.async_receive(current);
		},
		[&](const bii4::address& address, const beam::queue::common::port&)
		{
		    ASSERT_EQ(16777343U, address) << "Connection on unexpected address";
		    ++connection_count;
		},
		[&](const bii4::address&, const beam::queue::common::port&)
		{
		    GTEST_FATAL_FAILURE_("Unexpected disconnect");
		},
		[&](bqu::UnreliableMsg::Reader)
		{
		    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
		},
		[&](bqu::ReliableMsg::Reader)
		{
		    GTEST_FATAL_FAILURE_("Unexpected reliable message");
		}
	    });
	    master.service.run();
	}
	master.service.reset();
	std::size_t reliable_count = 0;
	while (reliable_count == 0)
	{
	    master.receiver.async_receive(
	    {
		[&](const receiver_master::receiver_type::event_handlers& current)
		{
		    std::unique_ptr<capnp::MallocMessageBuilder> builder(new capnp::MallocMessageBuilder());
		    bqu::ReliableMsg::Builder message = builder->initRoot<bqu::ReliableMsg>();
		    message.setValue("foo");
		    slave.send_reliable(std::move(builder));
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
		[&](bqu::UnreliableMsg::Reader)
		{
		    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
		},
		[&](bqu::ReliableMsg::Reader reader)
		{
		    ASSERT_STREQ("foo", reader.getValue().cStr()) << "Incorrect message value";
		    ++reliable_count;
		}
	    });
	    master.service.run();
	};
	slave.stop();
    }
}
