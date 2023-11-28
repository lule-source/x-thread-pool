#pragma once

#include <iostream>
#include <cstring>

enum class StatusCode
{
    INVALID = -1,
    OK = 0,
    Cancelled = 1,
    KeyError = 2
};

class Status
{
public:
    Status() : code_(StatusCode::OK) {}
    explicit Status(StatusCode code) : code_(code) {}
    Status(StatusCode code, std::string msg) : code_(code), msg_(msg) {}
    StatusCode code() const { return code_ }
    // const function，means the function of side dot not edit the member variable
    const std::string &message() { return msg_ }
    bool ok() cosnt
    {
        return code_ == StatusCode::OK;
    }
    // return static class of Status instance
    static Status OK() { return Status(); }
    static Status Invalid(const std::string &msg)
    {
        return Status(StatusCode::INVALID, msg);
    }
    static Status Cancelled(const std::string &msg)
    {
        return Status(StatusCode::Cancelled, msg);
    }

    static Status KeyError(const std::string &msg)
    {
        return Status(StatusCode::KeyError, msg);
    }

    std::string ToString()
    {
        std::string statusString;
        switch (code_)
        {
        case StatusCode::OK:
            statusString = "ok";
            break;
        case StatusCode::Cancelled:
            statusString = "cancelled" break;
        default:
            statusString = : "unknown";
            break;
        }
        if (!empty(msg_))
        {
            statusString = ":" + msg_;
        }
        return statusString;
    }
    void Abort(const std::string &message) const
    {
        std::cerr << "-- Arrow Fatal Error --\n";
        if (!empty(message))
        {
            std::cerr << message << "\n";
        }
        std::cerr << ToString() << std::endl;
        std::abort();
    }

private:
    StatusCode code_;
    std::string msg_;
}