#define BOOST_TEST_MODULE response_reader
#include <boost/test/included/unit_test.hpp>

#include <boost/asio/spawn.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>
#include "../src/or_throw.h"
#include "../src/response_reader.h"
#include "../src/util/wait_condition.h"

using namespace std;
using namespace ouinet;

using tcp = asio::ip::tcp;

// TODO: There should be a more straight forward way to do this.
tcp::socket
stream(string response, asio::io_service& ios, asio::yield_context yield)
{
    tcp::acceptor a(ios, tcp::endpoint(tcp::v4(), 0));
    tcp::socket s1(ios), s2(ios);

    sys::error_code accept_ec;
    sys::error_code connect_ec;

    WaitCondition wc(ios);

    asio::spawn(ios, [&, lock = wc.lock()] (asio::yield_context yield) mutable {
            a.async_accept(s2, yield[accept_ec]);
        });

    s1.async_connect(a.local_endpoint(), yield[connect_ec]);
    wc.wait(yield);

    if (accept_ec)  return or_throw(yield, accept_ec, move(s1));
    if (connect_ec) return or_throw(yield, connect_ec, move(s1));

    asio::spawn(ios, [rsp = move(response), s = move(s2)]
                     (asio::yield_context yield) mutable {
            asio::async_write(s, asio::buffer(rsp), yield);
        });

    return move(s1);
}

Http::Rsp::Part body(boost::string_view s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    return Http::Rsp::Body(vector<uint8_t>(p, p + s.size()));
}

Http::Rsp::Part chunk_data(boost::string_view s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    return Http::Rsp::ChunkBody(p, p + s.size());
}

Http::Rsp::Part chunk_hdr(size_t size, boost::string_view s) {
    return Http::Rsp::ChunkHdr{size, s.to_string()};
}

namespace ouinet { namespace Http { namespace Rsp {
    bool operator==(const Head&, const Head&) { return false; /* TODO */ }
    bool operator==(const Trailer&, const Trailer&) { return false; /* TODO */ }

    std::ostream& operator<<(std::ostream& os, const Head&) {
        return os << "Head";
    }
    
    std::ostream& operator<<(std::ostream& os, const ChunkHdr& hdr) {
        return os << "ChunkHdr(" << hdr.size << " exts:\"" << hdr.exts << "\")";
    }
    
    std::ostream& operator<<(std::ostream& os, const ChunkBody&) {
        return os << "ChunkBody";
    }
    
    std::ostream& operator<<(std::ostream& os, const Body&) {
        return os << "Body";
    }
    
    std::ostream& operator<<(std::ostream& os, const Trailer&) {
        return os << "Trailer";
    }
}}} // namespaces

BOOST_AUTO_TEST_SUITE(ouinet_response_reader)

BOOST_AUTO_TEST_CASE(test_http11_body) {
    namespace Rsp = Http::Rsp;

    asio::io_service ios;

    asio::spawn(ios, [&] (auto y_) {
        Yield y(ios, y_);

        string rsp =
            "HTTP/1.1 200 OK\r\n"
            "Date: Mon, 27 Jul 2019 12:30:20 GMT\r\n"
            "Content-Type: text/html\r\n"
            "Content-Length: 10\r\n"
            "\r\n"
            "0123456789";

        auto rsp_s = stream(move(rsp), ios, y);
        ResponseReader rr(move(rsp_s));

        Cancel c;
        Rsp::Part part;

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(get<Rsp::Head>(&part));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, body("0123456789"));
    });

    ios.run();
}

BOOST_AUTO_TEST_CASE(test_http11_chunk) {
    namespace Rsp = Http::Rsp;

    asio::io_service ios;

    asio::spawn(ios, [&] (auto y_) {
        Yield y(ios, y_);

        string rsp =
            "HTTP/1.1 200 OK\r\n"
            "Date: Mon, 27 Jul 2019 12:30:20 GMT\r\n"
            "Content-Type: text/html\r\n"
            "Transfer-Encoding: chunked\r\n"
            "\r\n"
            "4\r\n"
            "1234\r\n"
            "0\r\n"
            "\r\n";

        auto rsp_s = stream(move(rsp), ios, y);
        ResponseReader rr(move(rsp_s));

        Cancel c;
        Rsp::Part part;

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(get<Rsp::Head>(&part));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_hdr(4, ""));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_data("1234"));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_hdr(0, ""));
    });

    ios.run();
}

BOOST_AUTO_TEST_SUITE_END()


