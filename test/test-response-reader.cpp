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
using RR = ResponseReader;

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

vector<uint8_t> str_to_vec(boost::string_view s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    return {p, p + s.size()};
}

string vec_to_str(const vector<uint8_t>& v) {
    const char* p = reinterpret_cast<const char*>(v.data());
    return {p, v.size()};
}

map<string, string> fields_to_map(http::fields fields) {
    map<string, string> ret;
    for (auto& f : fields) {
        ret.insert({f.name_string().to_string(), f.value().to_string()}); 
    }
    return ret;
}

RR::Part body(boost::string_view s) {
    return RR::Body(str_to_vec(s));
}

RR::Part chunk_body(boost::string_view s) {
    return RR::ChunkBody(str_to_vec(s), 0);
}

RR::Part chunk_hdr(size_t size, boost::string_view s) {
    return RR::ChunkHdr{size, s.to_string()};
}

RR::Part trailer(map<string, string> trailer) {
    http::fields fields;
    for (auto& p : trailer) {
        fields.insert(p.first, p.second);
    }
    return RR::Trailer{move(fields)};
}

namespace ouinet {
    bool operator==(const RR::Head&, const RR::Head&) { return false; /* TODO */ }

    bool operator==(const RR::Trailer& t1, const RR::Trailer& t2) {
        return fields_to_map(t1) == fields_to_map(t2);
    }

    std::ostream& operator<<(std::ostream& os, const RR::Head&) {
        return os << "Head";
    }
    
    std::ostream& operator<<(std::ostream& os, const RR::ChunkHdr& hdr) {
        return os << "ChunkHdr(" << hdr.size << " exts:\"" << hdr.exts << "\")";
    }
    
    std::ostream& operator<<(std::ostream& os, const RR::ChunkBody& b) {
        return os << "ChunkBody(" << vec_to_str(b) << ")";
    }
    
    std::ostream& operator<<(std::ostream& os, const RR::Body& b) {
        return os << "Body(" << vec_to_str(b) << ")";
    }
    
    std::ostream& operator<<(std::ostream& os, const RR::Trailer&) {
        return os << "Trailer";
    }
} // ouinet namespaces

bool is_end_of_stream(RR& rr, Cancel& c, Yield& y) {
    sys::error_code ec;
    rr.async_read_part(c, y[ec]);
    return ec == http::error::end_of_stream;
}

RR::Part read_full_body(RR& rr, Cancel& c, Yield& y) {
    RR::Body body;

    while (true) {
        sys::error_code ec;
        auto part = rr.async_read_part(c, y[ec]);
        BOOST_REQUIRE(!ec);
        auto body_p = part.as_body();
        BOOST_REQUIRE(body_p);
        if (body_p->empty()) break;
        body.insert(body.end(), body_p->begin(), body_p->end());
    }

    return body;
}

BOOST_AUTO_TEST_SUITE(ouinet_response_reader)

BOOST_AUTO_TEST_CASE(test_http11_body) {
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

        RR rr(stream(move(rsp), ios, y));

        Cancel c;
        RR::Part part;

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(part.is_head());

        part = read_full_body(rr, c, y);
        BOOST_REQUIRE_EQUAL(part, body("0123456789"));

        BOOST_REQUIRE(is_end_of_stream(rr, c, y));
    });

    ios.run();
}

BOOST_AUTO_TEST_CASE(test_http11_chunk) {
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

        RR rr(stream(move(rsp), ios, y));

        Cancel c;
        RR::Part part;

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(part.is_head());

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_hdr(4, ""));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_body("1234"));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_hdr(0, ""));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, trailer({}));

        BOOST_REQUIRE(is_end_of_stream(rr, c, y));
    });

    ios.run();
}

BOOST_AUTO_TEST_CASE(test_http11_trailer) {
    asio::io_service ios;

    asio::spawn(ios, [&] (auto y_) {
        Yield y(ios, y_);

        string rsp =
            "HTTP/1.1 200 OK\r\n"
            "Date: Mon, 27 Jul 2019 12:30:20 GMT\r\n"
            "Content-Type: text/html\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Trailer: Hash\r\n"
            "\r\n"
            "4\r\n"
            "1234\r\n"
            "0\r\n"
            "Hash: hash_of_1234\r\n"
            "\r\n";

        RR rr(stream(move(rsp), ios, y));

        Cancel c;
        RR::Part part;

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(part.is_head());

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_hdr(4, ""));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_body("1234"));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, chunk_hdr(0, ""));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE_EQUAL(part, trailer({{"Hash", "hash_of_1234"}}));

        BOOST_REQUIRE(is_end_of_stream(rr, c, y));
    });

    ios.run();
}

BOOST_AUTO_TEST_CASE(test_http11_restart_body_body) {
    asio::io_service ios;

    asio::spawn(ios, [&] (auto y_) {
        Yield y(ios, y_);

        string rsp =
            "HTTP/1.1 200 OK\r\n"
            "Date: Mon, 27 Jul 2019 12:30:20 GMT\r\n"
            "Content-Type: text/html\r\n"
            "Content-Length: 10\r\n"
            "\r\n"
            "0123456789"

            "HTTP/1.1 200 OK\r\n"
            "Date: Mon, 27 Jul 2019 12:30:21 GMT\r\n"
            "Content-Type: text/html\r\n"
            "Content-Length: 5\r\n"
            "\r\n"
            "abcde";

        RR rr(stream(move(rsp), ios, y));

        Cancel c;
        RR::Part part;

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(part.is_head());

        part = read_full_body(rr, c, y);
        BOOST_REQUIRE_EQUAL(part, body("0123456789"));

        part = rr.async_read_part(c, y);
        BOOST_REQUIRE(part.is_head());

        part = read_full_body(rr, c, y);
        BOOST_REQUIRE_EQUAL(part, body("abcde"));

        BOOST_REQUIRE(is_end_of_stream(rr, c, y));
    });

    ios.run();
}

BOOST_AUTO_TEST_CASE(test_http11_restart_chunks_body) {
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
            "\r\n"

            "HTTP/1.1 200 OK\r\n"
            "Date: Mon, 27 Jul 2019 12:30:21 GMT\r\n"
            "Content-Type: text/html\r\n"
            "Content-Length: 5\r\n"
            "\r\n"
            "abcde";

        RR rr(stream(move(rsp), ios, y));

        Cancel c;
        RR::Part part;

        {
            part = rr.async_read_part(c, y);
            BOOST_REQUIRE(part.is_head());

            part = rr.async_read_part(c, y);
            BOOST_REQUIRE_EQUAL(part, chunk_hdr(4, ""));

            part = rr.async_read_part(c, y);
            BOOST_REQUIRE_EQUAL(part, chunk_body("1234"));

            part = rr.async_read_part(c, y);
            BOOST_REQUIRE_EQUAL(part, chunk_hdr(0, ""));

            part = rr.async_read_part(c, y);
            BOOST_REQUIRE_EQUAL(part, trailer({}));
        }

        {
            part = rr.async_read_part(c, y);
            BOOST_REQUIRE(part.is_head());

            part = read_full_body(rr, c, y);
            BOOST_REQUIRE_EQUAL(part, body("abcde"));
        }

        BOOST_REQUIRE(is_end_of_stream(rr, c, y));
    });

    ios.run();
}

BOOST_AUTO_TEST_SUITE_END()


