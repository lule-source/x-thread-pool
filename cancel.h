#pragma once

#include <functional>
#include <memory>

#include "status.h"

namespace arrow
{
    class StopToken;
    struct StopSourceImpl;

    class ARROW_EXPORT StopSource
    {
    public:
        StopSource();
        ~StopSource();
        void RequestStop();
        void RequestStop(Status error);
        void RequestStopFromSignal(int signum);

        StopToken token();
        void Reset();

    protected:
        std::shared_ptr<StopSourceImpl> impl_;
    };
    class ARROW_EXPORT StopToken
    {
    public:
        StopToken() {}
        explicit StopToken(std::shared_ptr<StopSourceImpl> impl) : impl_(std::move(impl)) {}
        static StopToken UnStopable() { return StopToken(); }
        Status Poll() const;
        bool IsStopRequested() const;

    protected:
        std::shared_ptr<StopSourceImpl> impl_;
    };

}