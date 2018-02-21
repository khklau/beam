#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <asio/io_service.hpp>
#include <beam/message/buffer_pool.hpp>
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
    typedef turbo::container::spsc_ring_queue<bme::unique_pool_ptr> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<bme::unique_pool_ptr> reliable_queue_type;
    initiator_slave(bdc::endpoint_id id, bdu::perf_params&& params);
    ~initiator_slave();
    inline bool is_running() const { return thread_ != nullptr; }
    void start();
    void stop();
    void send_unreliable(bme::capnproto<bdu::UnreliableMsg>& message);
    void send_reliable(bme::capnproto<bdu::ReliableMsg>& message);
    unreliable_queue_type::consumer::result try_receive_unreliable(bme::unique_pool_ptr& output);
    reliable_queue_type::consumer::result try_receive_reliable(bme::unique_pool_ptr& output);
private:
    initiator_slave() = delete;
    initiator_slave(const initiator_slave& other) = delete;
    initiator_slave& operator=(const initiator_slave& other) = delete;
    void run();
    void brake();
    void on_send_unreliable(out_connection_type& connection);
    void on_send_reliable(out_connection_type& connection);
    bdc::endpoint_id id_;
    std::thread* thread_;
    asio::io_service service_;
    asio::io_service::strand strand_;
    bme::buffer_pool pool_;
    initiator_type initiator_;
    unreliable_queue_type unreliable_in_queue_;
    reliable_queue_type reliable_in_queue_;
    unreliable_queue_type unreliable_out_queue_;
    reliable_queue_type reliable_out_queue_;
    in_connection_type::event_handlers handlers_;
};

initiator_slave::initiator_slave(bdc::endpoint_id id, bdu::perf_params&& params) :
	id_(id),
	thread_(nullptr),
	service_(),
	strand_(service_),
	pool_(0U, params.window_size),
	initiator_(strand_, std::move(params)),
	unreliable_in_queue_(128),
	reliable_in_queue_(128),
	unreliable_out_queue_(128),
	reliable_out_queue_(128),
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
	    [&](const in_connection_type&, bme::unique_pool_ptr&& data)
	    {
		ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_in_queue_.get_producer().try_enqueue_move(std::move(data)))
			<< "Unreliable message enqueue failed";
		initiator_.async_receive(handlers_);
	    },
	    [&](const in_connection_type&, bme::unique_pool_ptr&& data)
	    {
		ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_in_queue_.get_producer().try_enqueue_move(std::move(data)))
			<< "Reliable message enqueue failed";
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

void initiator_slave::send_unreliable(bme::capnproto<bdu::UnreliableMsg>& message)
{
    bme::unique_pool_ptr buffer = pool_.borrow();
    *buffer = std::move(message.serialise());
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_out_queue_.get_producer().try_enqueue_move(std::move(buffer)))
	    << "Unreliable message enqueue failed";
    initiator_.async_send(std::bind(&initiator_slave::on_send_unreliable, this, std::placeholders::_1));
}

void initiator_slave::send_reliable(bme::capnproto<bdu::ReliableMsg>& message)
{
    bme::unique_pool_ptr buffer = pool_.borrow();
    *buffer = std::move(message.serialise());
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_out_queue_.get_producer().try_enqueue_move(std::move(buffer)))
	    << "Reliable message enqueue failed";
    initiator_.async_send(std::bind(&initiator_slave::on_send_reliable, this, std::placeholders::_1));
}

initiator_slave::unreliable_queue_type::consumer::result initiator_slave::try_receive_unreliable(bme::unique_pool_ptr& output)
{
    return unreliable_in_queue_.get_consumer().try_dequeue_move(output);
}

initiator_slave::reliable_queue_type::consumer::result initiator_slave::try_receive_reliable(bme::unique_pool_ptr& output)
{
    return reliable_in_queue_.get_consumer().try_dequeue_move(output);
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
    bme::unique_pool_ptr message;
    ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_out_queue_.get_consumer().try_dequeue_move(message))
	    << "Unreliable message dequeue failed";
    connection.send_unreliable(*message);
}

void initiator_slave::on_send_reliable(out_connection_type& connection)
{
    bme::unique_pool_ptr message;
    ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_out_queue_.get_consumer().try_dequeue_move(message))
	    << "Reliable message dequeue failed";
    connection.send_reliable(*message);
}

class responder_master
{
public:
    typedef bdu::in_connection<bdu::UnreliableMsg, bdu::ReliableMsg> in_connection_type;
    typedef bdu::out_connection<bdu::UnreliableMsg, bdu::ReliableMsg> out_connection_type;
    typedef bdu::responder<in_connection_type, out_connection_type> responder_type;
    responder_master(bdc::endpoint_id&& point, bdu::perf_params&& params);
    ~responder_master();
    void bind(bdc::endpoint_id&& point);
    void send_unreliable(const bdc::endpoint_id& point, bme::capnproto<bdu::UnreliableMsg>& message);
    void send_reliable(const bdc::endpoint_id& point, bme::capnproto<bdu::ReliableMsg>& message);
    asio::io_service service;
    asio::io_service::strand strand;
    bme::buffer_pool pool;
    responder_type responder;
    std::unordered_set<bdc::endpoint_id> known_endpoints;
private:
    typedef turbo::container::spsc_ring_queue<bme::unique_pool_ptr> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<bme::unique_pool_ptr> reliable_queue_type;
    void on_send_unreliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find);
    void on_send_reliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find);
    unreliable_queue_type unreliable_in_queue_;
    reliable_queue_type reliable_in_queue_;
    unreliable_queue_type unreliable_out_queue_;
    reliable_queue_type reliable_out_queue_;
};

responder_master::responder_master(bdc::endpoint_id&& point, bdu::perf_params&& params) :
	service(),
	strand(service),
	pool(0U, params.window_size),
	responder(strand, std::move(params)),
	known_endpoints(),
	unreliable_in_queue_(128),
	reliable_in_queue_(128),
	unreliable_out_queue_(128),
	reliable_out_queue_(128)
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

void responder_master::send_unreliable(const bdc::endpoint_id& point, bme::capnproto<bdu::UnreliableMsg>& message)
{
    bme::unique_pool_ptr buffer = pool.borrow();
    *buffer = message.serialise();
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_in_queue_.get_producer().try_enqueue_move(std::move(buffer)))
	    << "Unreliable message enqueue failed";
    responder.async_send(std::bind(&responder_master::on_send_unreliable, this, point, std::placeholders::_1));
}

void responder_master::send_reliable(const bdc::endpoint_id& point, bme::capnproto<bdu::ReliableMsg>& message)
{
    bme::unique_pool_ptr buffer = pool.borrow();
    *buffer = message.serialise();
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_in_queue_.get_producer().try_enqueue_move(std::move(buffer)))
	    << "Reliable message enqueue failed";
    responder.async_send(std::bind(&responder_master::on_send_reliable, this, point, std::placeholders::_1));
}

void responder_master::on_send_unreliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find)
{
    out_connection_type* connection = find(point);
    if (connection != nullptr)
    {
	bme::unique_pool_ptr message;
	ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_in_queue_.get_consumer().try_dequeue_move(message))
		<< "Unreliable message dequeue failed";
	connection->send_unreliable(*message);
    }
}

void responder_master::on_send_reliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find)
{
    out_connection_type* connection = find(point);
    if (connection != nullptr)
    {
	bme::unique_pool_ptr message;
	ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_in_queue_.get_consumer().try_dequeue_move(message))
		<< "Reliable message dequeue failed";
	connection->send_reliable(*message);
    }
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
	    [&](const ::responder_master::in_connection_type& in)
	    {
		master.known_endpoints.insert(in.get_endpoint_id());
		++connection_count;
	    },
	    [&](const ::responder_master::in_connection_type&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected disconnect");
	    },
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected reliable message");
	    }
	});
	master.service.run();
    }
    master.service.reset();
}

} // anonymous namespace

TEST(unordered_mixed_test, basic_initiated_unreliable)
{
    ::responder_master master({0U, 18801U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 18801U}, {24U, 64U});
    setupConnection(master, slave);
    std::size_t unreliable_count = 0;
    while (unreliable_count == 0)
    {
	master.responder.async_receive(
	{
	    [&](const ::responder_master::in_connection_type::event_handlers& current)
	    {
		bme::capnproto<bdu::UnreliableMsg> message(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = message.build();
		builder.setValue(123U);
		slave.send_unreliable(message);
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
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	    {
		bme::capnproto<bdu::UnreliableMsg> message(std::move(data));
		ASSERT_EQ(123U, message.read().getValue()) << "Incorrect message value";
		++unreliable_count;
	    },
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected reliable message");
	    }
	});
	master.service.run();
    };
    slave.stop();
}

TEST(unordered_mixed_test, basic_initiated_reliable)
{
    ::responder_master master({0U, 18802U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 18802U}, {24U, 64U});
    setupConnection(master, slave);
    bme::capnproto<bdu::ReliableMsg> message(std::move(master.pool.borrow()));
    bdu::ReliableMsg::Builder builder = message.build();
    builder.setValue("foo");
    slave.send_reliable(message);
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
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	    {
		bme::capnproto<bdu::ReliableMsg> message(std::move(data));
		ASSERT_STREQ("foo", message.read().getValue().cStr()) << "Incorrect message value";
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();

}

TEST(unordered_mixed_test, basic_initiated_mixed)
{
    ::responder_master master({0U, 18803U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 18803U}, {24U, 64U});
    setupConnection(master, slave);
    bme::capnproto<bdu::ReliableMsg> message1(std::move(master.pool.borrow()));
    bdu::ReliableMsg::Builder builder1 = message1.build();
    builder1.setValue("bar");
    slave.send_reliable(message1);
    bme::capnproto<bdu::UnreliableMsg> message2(std::move(master.pool.borrow()));
    bdu::UnreliableMsg::Builder builder2 = message2.build();
    builder2.setValue(999U);
    slave.send_unreliable(message2);
    std::size_t reliable_count = 0;
    std::size_t unreliable_count = 0;
    while (reliable_count == 0 || unreliable_count == 0)
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
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	    {
		bme::capnproto<bdu::UnreliableMsg> message(std::move(data));
		ASSERT_EQ(999U, message.read().getValue()) << "Incorrect unreliable message value";
		++unreliable_count;
	    },
	    [&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	    {
		bme::capnproto<bdu::ReliableMsg> message(std::move(data));
		ASSERT_STREQ("bar", message.read().getValue().cStr()) << "Incorrect reliable message value";
		++reliable_count;
	    }
	});
	master.service.run();
	master.service.reset();
    };
    slave.stop();
}

TEST(unordered_mixed_test, basic_responded_unreliable)
{
    ::responder_master master({0U, 18901U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 18901U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    bme::capnproto<bdu::UnreliableMsg> message(std::move(master.pool.borrow()));
    bdu::UnreliableMsg::Builder builder = message.build();
    builder.setValue(123U);
    master.send_unreliable(*iter, message);
    master.responder.async_receive(
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    bme::unique_pool_ptr data;
	    if (slave.try_receive_unreliable(data) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::UnreliableMsg> received(std::move(data));
		ASSERT_EQ(123U, received.read().getValue()) << "Incorrect unreliable message value";
	    }
	    else
	    {
		master.responder.async_receive(current);
	    }
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected connect");
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected disconnect");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected reliable message");
	}
    });
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, basic_responded_reliable)
{
    ::responder_master master({0U, 18902U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 18902U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    bme::capnproto<bdu::ReliableMsg> message(std::move(master.pool.borrow()));
    bdu::ReliableMsg::Builder builder = message.build();
    builder.setValue("abcxyz");
    master.send_reliable(*iter, message);
    master.responder.async_receive(
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    bme::unique_pool_ptr data;
	    if (slave.try_receive_reliable(data) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::ReliableMsg> received(std::move(data));
		ASSERT_STREQ("abcxyz", received.read().getValue().cStr()) << "Incorrect reliable message value";
	    }
	    else
	    {
		master.responder.async_receive(current);
	    }
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected connect");
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected disconnect");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected reliable message");
	}
    });
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, basic_responded_mixed)
{
    ::responder_master master({0U, 18903U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 18903U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    bme::capnproto<bdu::ReliableMsg> message1(std::move(master.pool.borrow()));
    bdu::ReliableMsg::Builder builder1 = message1.build();
    builder1.setValue("abcxyz");
    master.send_reliable(*iter, message1);
    bme::capnproto<bdu::UnreliableMsg> message2(std::move(master.pool.borrow()));
    bdu::UnreliableMsg::Builder builder2 = message2.build();
    builder2.setValue(123U);
    master.send_unreliable(*iter, message2);
    unsigned int unreliable_count = 0U;
    unsigned int reliable_count = 0U;
    master.responder.async_receive(
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    bme::unique_pool_ptr data1;
	    if (slave.try_receive_unreliable(data1) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::UnreliableMsg> received1(std::move(data1));
		ASSERT_EQ(123U, received1.read().getValue()) << "Incorrect unreliable message value";
		++unreliable_count;
	    }
	    bme::unique_pool_ptr data2;
	    if (slave.try_receive_reliable(data2) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::ReliableMsg> received2(std::move(data2));
		ASSERT_STREQ("abcxyz", received2.read().getValue().cStr()) << "Incorrect reliable message value";
		++reliable_count;
	    }
	    if (unreliable_count == 0U || reliable_count == 0U)
	    {
		master.responder.async_receive(current);
	    }
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected connect");
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected disconnect");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected reliable message");
	}
    });
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, request_reply_initiated_unreliable)
{
    ::responder_master master({0U, 19001U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19001U}, {24U, 64U});
    setupConnection(master, slave);
    ::responder_master::in_connection_type::event_handlers handlers
    {
	[&](const ::responder_master::in_connection_type::event_handlers&)
	{
	    bme::unique_pool_ptr data1;
	    if (slave.try_receive_unreliable(data1) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::UnreliableMsg> received1(std::move(data1));
		ASSERT_EQ(476U, received1.read().getValue()) << "Incorrect unreliable message reply value";
	    }
	    else
	    {
		bme::capnproto<bdu::UnreliableMsg> message(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = message.build();
		builder.setValue(456U);
		slave.send_unreliable(message);
		master.responder.async_receive(handlers);
	    }
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected connect");
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected disconnect");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::UnreliableMsg> request(std::move(data));
	    auto iter = master.known_endpoints.begin();
	    bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
	    bdu::UnreliableMsg::Builder builder = reply.build();
	    builder.setValue(request.read().getValue() + 20);
	    master.send_unreliable(*iter, reply);
	    master.responder.async_receive(handlers);
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected reliable message");
	}
    };
    master.responder.async_receive(handlers);
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, request_reply_initiated_reliable)
{
    ::responder_master master({0U, 19002U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19002U}, {24U, 64U});
    setupConnection(master, slave);
    ::responder_master::in_connection_type::event_handlers handlers
    {
	[&](const ::responder_master::in_connection_type::event_handlers&)
	{
	    bme::unique_pool_ptr data1;
	    if (slave.try_receive_reliable(data1) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::ReliableMsg> received1(std::move(data1));
		ASSERT_STREQ("testing", received1.read().getValue().cStr()) << "Incorrect reliable message reply value";
	    }
	    else
	    {
		bme::capnproto<bdu::ReliableMsg> message(std::move(master.pool.borrow()));
		bdu::ReliableMsg::Builder builder = message.build();
		builder.setValue("test");
		slave.send_reliable(message);
		master.responder.async_receive(handlers);
	    }
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected connect");
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected disconnect");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::ReliableMsg> request(std::move(data));
	    auto iter = master.known_endpoints.begin();
	    bme::capnproto<bdu::ReliableMsg> reply(std::move(master.pool.borrow()));
	    bdu::ReliableMsg::Builder builder = reply.build();
	    std::string tmp(request.read().getValue());
	    builder.setValue(tmp.append("ing").c_str());
	    master.send_reliable(*iter, reply);
	    master.responder.async_receive(handlers);
	}
    };
    master.responder.async_receive(handlers);
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, request_reply_initiated_mixed)
{
    ::responder_master master({0U, 19003U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19003U}, {24U, 64U});
    setupConnection(master, slave);
    std::size_t unreliable_count = 0U;
    std::size_t reliable_count = 0U;
    std::size_t reliable_attempt = 0U;
    ::responder_master::in_connection_type::event_handlers handlers
    {
	[&](const ::responder_master::in_connection_type::event_handlers&)
	{
	    bme::unique_pool_ptr data1;
	    if (slave.try_receive_reliable(data1) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::ReliableMsg> received1(std::move(data1));
		ASSERT_STREQ("testing", received1.read().getValue().cStr()) << "Incorrect reliable message reply value";
		++reliable_count;
	    }
	    else if (reliable_attempt == 0U)
	    {
		bme::capnproto<bdu::ReliableMsg> message(std::move(master.pool.borrow()));
		bdu::ReliableMsg::Builder builder = message.build();
		builder.setValue("test");
		slave.send_reliable(message);
		++reliable_attempt;
	    }
	    bme::unique_pool_ptr data2;
	    if (slave.try_receive_unreliable(data2) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::UnreliableMsg> received2(std::move(data2));
		ASSERT_EQ(476U, received2.read().getValue()) << "Incorrect unreliable message reply value";
		++unreliable_count;
	    }
	    else
	    {
		bme::capnproto<bdu::UnreliableMsg> message(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = message.build();
		builder.setValue(456U);
		slave.send_unreliable(message);
	    }
	    if (unreliable_count == 0U || reliable_count == 0U)
	    {
		master.responder.async_receive(handlers);
	    }
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected connect");
	},
	[&](const ::responder_master::in_connection_type&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected disconnect");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::UnreliableMsg> request(std::move(data));
	    auto iter = master.known_endpoints.begin();
	    bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
	    bdu::UnreliableMsg::Builder builder = reply.build();
	    builder.setValue(request.read().getValue() + 20);
	    master.send_unreliable(*iter, reply);
	    master.responder.async_receive(handlers);
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::ReliableMsg> request(std::move(data));
	    auto iter = master.known_endpoints.begin();
	    bme::capnproto<bdu::ReliableMsg> reply(std::move(master.pool.borrow()));
	    bdu::ReliableMsg::Builder builder = reply.build();
	    std::string tmp(request.read().getValue());
	    builder.setValue(tmp.append("ing").c_str());
	    master.send_reliable(*iter, reply);
	    master.responder.async_receive(handlers);
	}
    };
    master.responder.async_receive(handlers);
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, request_reply_responded_unreliable)
{
    ::responder_master master({0U, 19101U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19101U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    bme::capnproto<bdu::UnreliableMsg> message(std::move(master.pool.borrow()));
    bdu::UnreliableMsg::Builder builder = message.build();
    builder.setValue(789U);
    master.send_unreliable(*iter, message);
    master.responder.async_receive(
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    bme::unique_pool_ptr data;
	    if (slave.try_receive_unreliable(data) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::UnreliableMsg> request(std::move(data));
		bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = reply.build();
		builder.setValue(request.read().getValue() + 10U);
		slave.send_unreliable(reply);
	    }
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
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::UnreliableMsg> reply(std::move(data));
	    ASSERT_EQ(799U, reply.read().getValue()) << "Incorrect unreliable message value";
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected reliable message");
	}
    });
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, request_reply_responded_reliable)
{
    ::responder_master master({0U, 19102U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19102U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    bme::capnproto<bdu::ReliableMsg> message(std::move(master.pool.borrow()));
    bdu::ReliableMsg::Builder builder = message.build();
    builder.setValue("compute");
    master.send_reliable(*iter, message);
    master.responder.async_receive(
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    bme::unique_pool_ptr data;
	    if (slave.try_receive_reliable(data) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::ReliableMsg> request(std::move(data));
		bme::capnproto<bdu::ReliableMsg> reply(std::move(master.pool.borrow()));
		bdu::ReliableMsg::Builder builder = reply.build();
		std::string tmp("pre-");
		builder.setValue(tmp.append(request.read().getValue()).c_str());
		slave.send_reliable(reply);
	    }
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
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::ReliableMsg> reply(std::move(data));
	    ASSERT_STREQ("pre-compute", reply.read().getValue().cStr()) << "Incorrect reliable message value";
	}
    });
    master.service.run();
    slave.stop();
}

TEST(unordered_mixed_test, request_reply_responded_mixed)
{
    ::responder_master master({0U, 19103U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19103U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    bme::capnproto<bdu::ReliableMsg> message1(std::move(master.pool.borrow()));
    bdu::ReliableMsg::Builder builder1 = message1.build();
    builder1.setValue("compute");
    master.send_reliable(*iter, message1);
    bme::capnproto<bdu::UnreliableMsg> message2(std::move(master.pool.borrow()));
    bdu::UnreliableMsg::Builder builder2 = message2.build();
    builder2.setValue(789U);
    master.send_unreliable(*iter, message2);
    std::size_t unreliable_count = 0U;
    std::size_t reliable_count = 0U;
    ::responder_master::in_connection_type::event_handlers handlers
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    bme::unique_pool_ptr data1;
	    if (slave.try_receive_unreliable(data1) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::UnreliableMsg> request1(std::move(data1));
		bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = reply.build();
		builder.setValue(request1.read().getValue() + 10U);
		slave.send_unreliable(reply);
	    }
	    bme::unique_pool_ptr data2;
	    if (slave.try_receive_reliable(data2) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto<bdu::ReliableMsg> request2(std::move(data2));
		bme::capnproto<bdu::ReliableMsg> reply(std::move(master.pool.borrow()));
		bdu::ReliableMsg::Builder builder = reply.build();
		std::string tmp("pre-");
		builder.setValue(tmp.append(request2.read().getValue()).c_str());
		slave.send_reliable(reply);
	    }
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
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::UnreliableMsg> reply(std::move(data));
	    ASSERT_EQ(799U, reply.read().getValue()) << "Incorrect unreliable message value";
	    ++unreliable_count;
	    if (unreliable_count == 0U || reliable_count == 0U)
	    {
		master.responder.async_receive(handlers);
	    }
	},
	[&](const ::responder_master::in_connection_type&, bme::unique_pool_ptr&& data)
	{
	    bme::capnproto<bdu::ReliableMsg> reply(std::move(data));
	    ASSERT_STREQ("pre-compute", reply.read().getValue().cStr()) << "Incorrect reliable message value";
	    ++reliable_count;
	    if (unreliable_count == 0U || reliable_count == 0U)
	    {
		master.responder.async_receive(handlers);
	    }
	}
    };
    master.responder.async_receive(handlers);
    master.service.run();
    slave.stop();
}
