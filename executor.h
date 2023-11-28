
#pragma once
#include "cancel.h"
#include "status.h"

namespace arrow
{
    struct TaskHints
    {
        int32_t priority = 0;
        int64_t io_size = -1;
        int64_t cpu_cost = -1;
        int64_t external_id = -1;
    };

    class ARROW_EXPORT Executor
    {
    public:
        using StopCallback = internal::FnOnce<void(const Status &)>;

        virtual ~Executor();

        template <typename Function>
        Status Spawn(Function &&func)
        {
            return SpawnReal(TaskHints{}, std::forward<Function>(func), StopToken::Unstoppable(),
                             StopCallback{});
        }
        template <typename Function>
        Status Spawn(Function &&func, StopToken stop_token)
        {
            return SpawnReal(TaskHints{}, std::forward<Function>(func), std::move(stop_token),
                             StopCallback{});
        }
        template <typename Function>
        Status Spawn(TaskHints hints, Function &&func)
        {
            return SpawnReal(hints, std::forward<Function>(func), StopToken::Unstoppable(),
                             StopCallback{});
        }
        template <typename Function>
        Status Spawn(TaskHints hints, Function &&func, StopToken stop_token)
        {
            return SpawnReal(hints, std::forward<Function>(func), std::move(stop_token),
                             StopCallback{});
        }
        template <typename Function>
        Status Spawn(TaskHints hints, Function &&func, StopToken stop_token,
                     StopCallback stop_callback)
        {
            return SpawnReal(hints, std::forward<Function>(func), std::move(stop_token),
                             std::move(stop_callback));
        }

        template <typename Function, typename... Args,
                  typename ReturnType = typename std::result_of<Function(Args...)>::type>
        std::future<ReturnType> Submit(TaskHints hints, StopToken stop_token,
                                       StopCallback stop_callback, Function &&func,
                                       Args &&...args)
        {
            /**
             * std::promise 和 std::future 提供了一种方便的方式，
             * 在异步编程中进行线程间通信和结果传递。生产者线程使用 std::promise 设置结果或异常，
             * 而消费者线程使用 std::future 获取结果或异常。
             * 这种机制使得异步操作的结果可以方便地传递给其他线程进行处理。
             */
            std::promise<ReturnType> promise;
            std::future<ReturnType> future = promise.get_future();
            auto task = [func = std::forward<Function>(func),
                         tup = std::make_tuple(std::forward<Args>(args)...),
                         promise = std::move(promise)]() mutable
            {
                try
                {
                    if constexpr (!std::is_void_v<ReturnType>)
                    {
                        ReturnType result = std::apply(std::move(func), std::move(tup));
                        promise.set_value(result);
                    }
                    else
                    {
                        // 用于将一个函数和一组参数打包为一个
                        // std::tuple，然后通过调用该函数来应用这组参数
                        std::apply(std::move(func),
                                   std::move(tup));
                    }
                }
                catch (...)
                {
                    promise.set_exception(std::current_exception());
                }
            };

            Status status = SpawnReal(hints, std::move(task), stop_token, std::move(stop_callback));
            if (!status.ok())
            {
                throw std::runtime_error("Failed to submit task");
            }

            return future;
        }
        template <typename Function, typename... Args,
                  typename ReturnType = typename std::result_of<Function(Args...)>::type>
        std::future<ReturnType> Submit(StopToken stop_token, Function &&func, Args &&...args)
        {
            return Submit(TaskHints{}, stop_token, StopCallback{}, std::forward<Function>(func),
                          std::forward<Args>(args)...);
        }

        template <typename Function, typename... Args,
                  typename ReturnType = typename std::result_of<Function(Args...)>::type>
        std::future<ReturnType> Submit(TaskHints hints, Function &&func, Args &&...args)
        {
            return Submit(std::move(hints), StopToken::Unstoppable(), StopCallback{},
                          std::forward<Function>(func), std::forward<Args>(args)...);
        }

        template <typename Function, typename... Args,
                  typename ReturnType = typename std::result_of<Function(Args...)>::type>
        std::future<ReturnType> Submit(StopCallback stop_callback, Function &&func,
                                       Args &&...args)
        {
            return Submit(TaskHints{}, StopToken::Unstoppable(), stop_callback,
                          std::forward<Function>(func), std::forward<Args>(args)...);
        }

        template <typename Function, typename... Args,
                  typename ReturnType = typename std::result_of<Function(Args...)>::type>
        std::future<ReturnType> Submit(Function &&func, Args &&...args)
        {
            return Submit(TaskHints{}, StopToken::Unstoppable(), StopCallback{},
                          std::forward<Function>(func), std::forward<Args>(args)...);
        }

        // Subclassing API
        virtual Status SpawnReal(TaskHints hints, internal::FnOnce<void()> task, StopToken,
                                 StopCallback &&) = 0;
    };
}