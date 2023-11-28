#pragma once

#include <unistd.h>

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <queue>
#include <type_traits>
#include <utility>

#include "cancel.h"
#include "functional.h"
#include "status.h"
#include "visibility.h"
#include "executor.h"

namespace arrow
{

    class StopToken;
    ARROW_EXPORT int GetCpuThreadPoolCapacity();
    ARROW_EXPORT Status SetCpuThreadPoolCapacity(int threads);

    class ARROW_EXPORT ThreadPool : public Executor
    {
    public:
        static std::optional<std::shared_ptr<ThreadPool>> Make(int threads);
        static std::optional<std::shared_ptr<ThreadPool>> MakeEternal(int threads);

        ~ThreadPool();
        int GetCapacity();
        bool OwnsThisThread();
        int GetNumTasks();

        Status SetCapacity(int threads);
        static int DefaultCapacity();

        Status Shutdown(bool wait = true);

        void WaitForIdle();

        struct State;

    protected:
        friend ARROW_EXPORT ThreadPool *GetCpuThreadPool();

        ThreadPool();

        Status SpawnReal(TaskHints hints, internal::FnOnce<void()> task, StopToken,
                         StopCallback &&);

        void CollectFinishedWorkersUnlocked();
        void LaunchWorkersUnlocked(int threads);
        int GetActualCapacity();
        void ProtectAgainstFork();

        static std::shared_ptr<ThreadPool> MakeCpuThreadPool();

        std::shared_ptr<State> sp_state_;
        State *state_;
        bool shutdown_on_destroy_;
        pid_t pid_;
    };

    ARROW_EXPORT ThreadPool *GetCpuThreadPool();
}
