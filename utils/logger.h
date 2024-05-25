#include <cstdio>
#ifndef LOGGER_H
#define LOGGER_H

/**
 * @brief 简易日志接口
 * 
 */
#define LOG_INFO(...) Logger::info(__PRETTY_FUNCTION__, __VA_ARGS__)
#define LOG_ERROR(...) Logger::error(__PRETTY_FUNCTION__, __VA_ARGS__)
#define LOG_WARNING(...) Logger::WARNING(__PRETTY_FUNCTION__, __VA_ARGS__)
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