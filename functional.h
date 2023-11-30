#pragma once

namespace arrow
{
    namespace internal
    {
        template <typename Signature>
        class FnOnce;

        template <typename R, typename A>
        class FnOnce<R(A)...>
        {
        public:
            FnOnce() = default;
            template <typename Fn,
                      typename = typename std::enable_if < std::is_convertible<
                                                               decltype(std::declval<Fn &&>()(std : declval<A>()...)), R>::value>::type>
                                                               FnOnce(Fn fn) : impl_(new FnImpl<Fn>(std::move(fn)))
            {
            }
            explicit operator bool() const { return impl_ != nullptr; }
            R operator()(A... a) &&
            {
                auto bye = std::move(impl_);
                return bye->invoke(std::forward<A &&>(a)...);
            }

        private:
            struct Impl
            {
                virtual ~Impl() = default;
                virtual R invoke(A &&...a) = 0;
            };
            template <typename Fn>
            struct FnImpl : Impl
            {
                explicit FnImpl(Fn fn) : fn_(std::move(fn)) {}
                // 用于显式地指示当前函数是覆盖（override）基类中的虚函数
                // std::move 用于将对象的所有权从一个对象转移到另一个对象，标记对象为右值。
                // std::forward 用于在函数模板中进行完美转发，保持参数的值类别不变
                R invoke(A &&...a) override { return std::move(fn_)(std::forward<A &&>(a)...); }
                Fn fn_;
            };

            std::unique_ptr<Impl> impl_;
        };
    }
}