#include <cstdio>
#ifndef LOGGER_H
#define LOGGER_H
/**
 * @brief 日志类
 * 
 */
class Logger {
public: 
    static void info(const char *format, ...);
    static void error(const char *format, ...);
    static void warning(const char *format, ...); 

private:
    static void log(FILE *output, const char *type, const char *format, va_list args);
};

#endif