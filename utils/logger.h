#include <cstdio>
#ifndef LOGGER_H
#define LOGGER_H
#define ENABLE_LOG
/**
 * @brief 简易日志接口
 * 
 */
#ifdef ENABLE_LOG
#define LOG_INFO(...) Logger::info(__PRETTY_FUNCTION__, __VA_ARGS__)
#define LOG_ERROR(...) Logger::error(__PRETTY_FUNCTION__, __VA_ARGS__)
#define LOG_WARNING(...) Logger::warning(__PRETTY_FUNCTION__, __VA_ARGS__)
#else
#define LOG_INFO(...)
#define LOG_ERROR(...)
#define LOG_WARNING(...)
#endif

/**
 * @brief 日志类
 * 
 */
class Logger {
public: 
    static void info(const char *func, const char *format, ...);
    static void error(const char *func, const char *format, ...);
    static void warning(const char *func, const char *format, ...); 

private:
    static void log(FILE *output, const char *type, const char *func, const char *format, va_list args);
};

#endif