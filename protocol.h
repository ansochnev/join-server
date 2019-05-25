#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <iostream>
#include <string>
#include <vector>
#include <memory>

#include <boost/asio.hpp>

namespace proto 
{
const std::string INSERT       = "INSERT";
const std::string TRUNCATE     = "TRUNCATE";
const std::string INTERSECTION = "INTERSECTION";
const std::string SYMDIFF      = "SYMMETRIC_DIFFERENCE";


struct Request
{
    std::string query;
};


class IResponseWriter
{
public:
    virtual void writeError(const std::string& message) = 0;
    virtual void write(const std::string& data)         = 0;
    virtual ~IResponseWriter() = default;
};


class IHandler
{
public:
    virtual void handle(IResponseWriter* rw, Request& req) = 0;
    virtual ~IHandler() = default;
};


class Server
{
    boost::asio::io_service        m_service;
    boost::asio::ip::tcp::acceptor m_acceptor;
    boost::asio::ip::tcp::socket   m_socket;

    IHandler *m_handler;

public:
    Server(short port, IHandler* handler);
    ~Server();

    void run();

private:
    void do_accept();
};

} // namespace proto

#endif // PROTOCOL_H