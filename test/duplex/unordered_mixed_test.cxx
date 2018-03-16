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
    typedef turbo::container::spsc_ring_queue<bme::payload<bdu::UnreliableMsg>> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<bme::payload<bdu::ReliableMsg>> reliable_queue_type;
    initiator_slave(bdc::endpoint_id id, bdu::perf_params&& params);
    ~initiator_slave();
    inline bool is_running() const { return thread_ != nullptr; }
    void start();
    void stop();
    void send_unreliable(bme::capnproto<bdu::UnreliableMsg>& message);
    void send_reliable(bme::capnproto<bdu::ReliableMsg>& message);
    unreliable_queue_type::consumer::result try_receive_unreliable(bme::payload<bdu::UnreliableMsg>& output);
    reliable_queue_type::consumer::result try_receive_reliable(bme::payload<bdu::ReliableMsg>& output);
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
    unreliable_queue_type::producer& unreliable_in_producer_;
    unreliable_queue_type::consumer& unreliable_in_consumer_;
    reliable_queue_type::producer& reliable_in_producer_;
    reliable_queue_type::consumer& reliable_in_consumer_;
    unreliable_queue_type::producer& unreliable_out_producer_;
    unreliable_queue_type::consumer& unreliable_out_consumer_;
    reliable_queue_type::producer& reliable_out_producer_;
    reliable_queue_type::consumer& reliable_out_consumer_;
    in_connection_type::event_handlers handlers_;
};

initiator_slave::initiator_slave(bdc::endpoint_id id, bdu::perf_params&& params) :
	id_(id),
	thread_(nullptr),
	service_(),
	strand_(service_),
	pool_(8U, params.window_size),
	initiator_(strand_, std::move(params)),
	unreliable_in_queue_(128),
	reliable_in_queue_(128),
	unreliable_out_queue_(128),
	reliable_out_queue_(128),
	unreliable_in_producer_(unreliable_in_queue_.get_producer()),
	unreliable_in_consumer_(unreliable_in_queue_.get_consumer()),
	reliable_in_producer_(reliable_in_queue_.get_producer()),
	reliable_in_consumer_(reliable_in_queue_.get_consumer()),
	unreliable_out_producer_(unreliable_out_queue_.get_producer()),
	unreliable_out_consumer_(unreliable_out_queue_.get_consumer()),
	reliable_out_producer_(reliable_out_queue_.get_producer()),
	reliable_out_consumer_(reliable_out_queue_.get_consumer()),
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
	    [&](const in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	    {
		ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_in_producer_.try_enqueue_move(std::move(payload)))
			<< "Unreliable message enqueue failed";
		initiator_.async_receive(handlers_);
	    },
	    [&](const in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	    {
		ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_in_producer_.try_enqueue_move(std::move(payload)))
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
    bme::payload<bdu::UnreliableMsg> payload(std::move(bme::serialise(pool_, message)));
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_out_producer_.try_enqueue_move(std::move(payload)))
	    << "Unreliable message enqueue failed";
    initiator_.async_send(std::bind(&initiator_slave::on_send_unreliable, this, std::placeholders::_1));
}

void initiator_slave::send_reliable(bme::capnproto<bdu::ReliableMsg>& message)
{
    bme::payload<bdu::ReliableMsg> payload(std::move(bme::serialise(pool_, message)));
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_out_producer_.try_enqueue_move(std::move(payload)))
	    << "Reliable message enqueue failed";
    initiator_.async_send(std::bind(&initiator_slave::on_send_reliable, this, std::placeholders::_1));
}

initiator_slave::unreliable_queue_type::consumer::result initiator_slave::try_receive_unreliable(bme::payload<bdu::UnreliableMsg>& output)
{
    return unreliable_in_consumer_.try_dequeue_move(output);
}

initiator_slave::reliable_queue_type::consumer::result initiator_slave::try_receive_reliable(bme::payload<bdu::ReliableMsg>& output)
{
    return reliable_in_consumer_.try_dequeue_move(output);
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
    bme::payload<bdu::UnreliableMsg> message;
    ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_out_consumer_.try_dequeue_move(message))
	    << "Unreliable message dequeue failed";
    connection.send_unreliable(message);
}

void initiator_slave::on_send_reliable(out_connection_type& connection)
{
    bme::payload<bdu::ReliableMsg> message;
    ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_out_consumer_.try_dequeue_move(message))
	    << "Reliable message dequeue failed";
    connection.send_reliable(message);
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
    typedef turbo::container::spsc_ring_queue<bme::payload<bdu::UnreliableMsg>> unreliable_queue_type;
    typedef turbo::container::spsc_ring_queue<bme::payload<bdu::ReliableMsg>> reliable_queue_type;
    void on_send_unreliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find);
    void on_send_reliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find);
    unreliable_queue_type unreliable_in_queue_;
    reliable_queue_type reliable_in_queue_;
    unreliable_queue_type unreliable_out_queue_;
    reliable_queue_type reliable_out_queue_;
    unreliable_queue_type::producer& unreliable_in_producer_;
    unreliable_queue_type::consumer& unreliable_in_consumer_;
    reliable_queue_type::producer& reliable_in_producer_;
    reliable_queue_type::consumer& reliable_in_consumer_;
    unreliable_queue_type::producer& unreliable_out_producer_;
    unreliable_queue_type::consumer& unreliable_out_consumer_;
    reliable_queue_type::producer& reliable_out_producer_;
    reliable_queue_type::consumer& reliable_out_consumer_;
};

responder_master::responder_master(bdc::endpoint_id&& point, bdu::perf_params&& params) :
	service(),
	strand(service),
	pool(8U, params.window_size),
	responder(strand, std::move(params)),
	known_endpoints(),
	unreliable_in_queue_(128),
	reliable_in_queue_(128),
	unreliable_out_queue_(128),
	reliable_out_queue_(128),
	unreliable_in_producer_(unreliable_in_queue_.get_producer()),
	unreliable_in_consumer_(unreliable_in_queue_.get_consumer()),
	reliable_in_producer_(reliable_in_queue_.get_producer()),
	reliable_in_consumer_(reliable_in_queue_.get_consumer()),
	unreliable_out_producer_(unreliable_out_queue_.get_producer()),
	unreliable_out_consumer_(unreliable_out_queue_.get_consumer()),
	reliable_out_producer_(reliable_out_queue_.get_producer()),
	reliable_out_consumer_(reliable_out_queue_.get_consumer())
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
    bme::payload<bdu::UnreliableMsg> payload(std::move(bme::serialise(pool, message)));
    ASSERT_EQ(unreliable_queue_type::producer::result::success, unreliable_in_producer_.try_enqueue_move(std::move(payload)))
	    << "Unreliable message enqueue failed";
    responder.async_send(std::bind(&responder_master::on_send_unreliable, this, point, std::placeholders::_1));
}

void responder_master::send_reliable(const bdc::endpoint_id& point, bme::capnproto<bdu::ReliableMsg>& message)
{
    bme::payload<bdu::ReliableMsg> payload(std::move(bme::serialise(pool, message)));
    ASSERT_EQ(reliable_queue_type::producer::result::success, reliable_in_producer_.try_enqueue_move(std::move(payload)))
	    << "Reliable message enqueue failed";
    responder.async_send(std::bind(&responder_master::on_send_reliable, this, point, std::placeholders::_1));
}

void responder_master::on_send_unreliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find)
{
    out_connection_type* connection = find(point);
    if (connection != nullptr)
    {
	bme::payload<bdu::UnreliableMsg> message;
	ASSERT_EQ(unreliable_queue_type::consumer::result::success, unreliable_in_consumer_.try_dequeue_move(message))
		<< "Unreliable message dequeue failed";
	connection->send_unreliable(message);
    }
}

void responder_master::on_send_reliable(bdc::endpoint_id point, std::function<out_connection_type*(const beam::duplex::common::endpoint_id&)> find)
{
    out_connection_type* connection = find(point);
    if (connection != nullptr)
    {
	bme::payload<bdu::ReliableMsg> message;
	ASSERT_EQ(reliable_queue_type::consumer::result::success, reliable_in_consumer_.try_dequeue_move(message))
		<< "Reliable message dequeue failed";
	connection->send_reliable(message);
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
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> deed(std::move(payload));
		ASSERT_EQ(123U, deed.read().getValue()) << "Incorrect message value";
		++unreliable_count;
	    },
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	    {
		GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	    },
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> deed(std::move(payload));
		ASSERT_STREQ("foo", deed.read().getValue().cStr()) << "Incorrect message value";
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
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> deed(std::move(payload));
		ASSERT_EQ(999U, deed.read().getValue()) << "Incorrect unreliable message value";
		++unreliable_count;
	    },
	    [&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> deed(std::move(payload));
		ASSERT_STREQ("bar", deed.read().getValue().cStr()) << "Incorrect reliable message value";
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
	    bme::payload<bdu::UnreliableMsg> payload;
	    if (slave.try_receive_unreliable(payload) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> deed(std::move(payload));
		ASSERT_EQ(123U, deed.read().getValue()) << "Incorrect unreliable message value";
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    bme::payload<bdu::ReliableMsg> payload;
	    if (slave.try_receive_reliable(payload) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> deed(std::move(payload));
		ASSERT_STREQ("abcxyz", deed.read().getValue().cStr()) << "Incorrect reliable message value";
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    bme::payload<bdu::UnreliableMsg> payload1;
	    if (slave.try_receive_unreliable(payload1) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> deed1(std::move(payload1));
		ASSERT_EQ(123U, deed1.read().getValue()) << "Incorrect unreliable message value";
		++unreliable_count;
	    }
	    bme::payload<bdu::ReliableMsg> payload2;
	    if (slave.try_receive_reliable(payload2) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> deed2(std::move(payload2));
		ASSERT_STREQ("abcxyz", deed2.read().getValue().cStr()) << "Incorrect reliable message value";
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    bme::payload<bdu::UnreliableMsg> payload1;
	    if (slave.try_receive_unreliable(payload1) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> deed1(std::move(payload1));
		ASSERT_EQ(476U, deed1.read().getValue()) << "Incorrect unreliable message reply value";
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::UnreliableMsg> request(std::move(payload));
	    auto iter = master.known_endpoints.begin();
	    bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
	    bdu::UnreliableMsg::Builder builder = reply.build();
	    builder.setValue(request.read().getValue() + 20);
	    master.send_unreliable(*iter, reply);
	    master.responder.async_receive(handlers);
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    bme::payload<bdu::ReliableMsg> payload1;
	    if (slave.try_receive_reliable(payload1) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> deed1(std::move(payload1));
		ASSERT_STREQ("testing", deed1.read().getValue().cStr()) << "Incorrect reliable message reply value";
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::ReliableMsg> request(std::move(payload));
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
	    bme::payload<bdu::ReliableMsg> payload1;
	    if (slave.try_receive_reliable(payload1) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> deed1(std::move(payload1));
		ASSERT_STREQ("testing", deed1.read().getValue().cStr()) << "Incorrect reliable message reply value";
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
	    bme::payload<bdu::UnreliableMsg> payload2;
	    if (slave.try_receive_unreliable(payload2) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> deed2(std::move(payload2));
		ASSERT_EQ(476U, deed2.read().getValue()) << "Incorrect unreliable message reply value";
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::UnreliableMsg> request(std::move(payload));
	    auto iter = master.known_endpoints.begin();
	    bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
	    bdu::UnreliableMsg::Builder builder = reply.build();
	    builder.setValue(request.read().getValue() + 20);
	    master.send_unreliable(*iter, reply);
	    master.responder.async_receive(handlers);
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::ReliableMsg> request(std::move(payload));
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
	    bme::payload<bdu::UnreliableMsg> payload;
	    if (slave.try_receive_unreliable(payload) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> request(std::move(payload));
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::UnreliableMsg> deed(std::move(payload));
	    ASSERT_EQ(799U, deed.read().getValue()) << "Incorrect unreliable message value";
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&&)
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
	    bme::payload<bdu::ReliableMsg> payload;
	    if (slave.try_receive_reliable(payload) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> request(std::move(payload));
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&&)
	{
	    GTEST_FATAL_FAILURE_("Unexpected unreliable message");
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::ReliableMsg> deed(std::move(payload));
	    ASSERT_STREQ("pre-compute", deed.read().getValue().cStr()) << "Incorrect reliable message value";
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
	    bme::payload<bdu::UnreliableMsg> payload1;
	    if (slave.try_receive_unreliable(payload1) == ::initiator_slave::unreliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::UnreliableMsg> request1(std::move(payload1));
		bme::capnproto<bdu::UnreliableMsg> reply(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = reply.build();
		builder.setValue(request1.read().getValue() + 10U);
		slave.send_unreliable(reply);
	    }
	    bme::payload<bdu::ReliableMsg> payload2;
	    if (slave.try_receive_reliable(payload2) == ::initiator_slave::reliable_queue_type::consumer::result::success)
	    {
		bme::capnproto_deed<bdu::ReliableMsg> request2(std::move(payload2));
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::UnreliableMsg> deed(std::move(payload));
	    ASSERT_EQ(799U, deed.read().getValue()) << "Incorrect unreliable message value";
	    ++unreliable_count;
	    if (unreliable_count == 0U || reliable_count == 0U)
	    {
		master.responder.async_receive(handlers);
	    }
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::ReliableMsg> deed(std::move(payload));
	    ASSERT_STREQ("pre-compute", deed.read().getValue().cStr()) << "Incorrect reliable message value";
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

TEST(unordered_mixed_test, recycled_buffers)
{
    ::responder_master master({0U, 19103U}, {24U, 64U});
    ::initiator_slave slave({::localhost, 19103U}, {24U, 64U});
    setupConnection(master, slave);
    auto iter = master.known_endpoints.begin();
    std::unordered_set<std::string> reliable_values({"abc", "xyz", "!@#", "*()"});
    auto reliable_iter = reliable_values.begin();
    std::unordered_set<uint32_t> unreliable_values({123U, 456U, 789U, 0U});
    auto unreliable_iter = unreliable_values.begin();
    std::size_t unreliable_count = 0;
    std::size_t unreliable_sent = 0;
    std::size_t reliable_count = 0;
    std::size_t reliable_sent = 0;
    std::size_t reliable_target = reliable_values.size();
    ::responder_master::in_connection_type::event_handlers handlers
    {
	[&](const ::responder_master::in_connection_type::event_handlers& current)
	{
	    if (reliable_iter != reliable_values.end() && reliable_sent <= reliable_count)
	    {
		bme::capnproto<bdu::ReliableMsg> message(std::move(master.pool.borrow()));
		bdu::ReliableMsg::Builder builder = message.build();
		builder.setValue(*reliable_iter);
		slave.send_reliable(message);
		++reliable_sent;
	    }
	    if (unreliable_iter != unreliable_values.end() && unreliable_sent <= unreliable_count)
	    {
		bme::capnproto<bdu::UnreliableMsg> message(std::move(master.pool.borrow()));
		bdu::UnreliableMsg::Builder builder = message.build();
		builder.setValue(*unreliable_iter);
		slave.send_unreliable(message);
		++unreliable_sent;
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
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::UnreliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::UnreliableMsg> deed(std::move(payload));
	    auto result = unreliable_values.find(deed.read().getValue());
	    ASSERT_NE(unreliable_values.end(), result) << "Incorrect unreliable message value";
	    ++unreliable_iter;
	    ++unreliable_count;
	    if (unreliable_count == 0 || reliable_count != reliable_target)
	    {
		master.responder.async_receive(handlers);
	    }
	},
	[&](const ::responder_master::in_connection_type&, bme::payload<bdu::ReliableMsg>&& payload)
	{
	    bme::capnproto_deed<bdu::ReliableMsg> deed(std::move(payload));
	    auto result = reliable_values.find(deed.read().getValue());
	    ASSERT_NE(reliable_values.end(), result) << "Incorrect reliable message value";
	    ++reliable_iter;
	    ++reliable_count;
	    if (unreliable_count == 0 || reliable_count != reliable_target)
	    {
		master.responder.async_receive(handlers);
	    }
	}
    };
    master.responder.async_receive(handlers);
    master.service.run();
    master.service.reset();
    slave.stop();
}
