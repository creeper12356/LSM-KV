#include "logger.h"
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <cstring>
#ifdef ENABLE_LOG
void Logger::info(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    log(stdout, "INFO", format, args);
    fprintf(stdout, "\n");
    va_end(args);
}

void Logger::error(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    fprintf(stderr, "\033[1;31m");
    log(stderr, "ERROR", format, args);
    fprintf(stderr, "\033[0m\n");
    va_end(args);
}

void Logger::warning(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    fprintf(stderr, "\033[1;35m");
    log(stderr, "WARNING", format, args);
    fprintf(stderr, "\033[0m\n");
    va_end(args);
}
void Logger::log(FILE *output, const char *type, const char *format, va_list args) {
    time_t now = time(0);
    char *dt = ctime(&now);
    dt[strlen(dt) - 1] = '\0';
    fprintf(output, "[%s] %s: ", dt, type);
    vfprintf(output, format, args);
}
#else 
void Logger::info(const char *format, ...)
{
}
void Logger::error(const char *format, ...)
{
}
void Logger::warning(const char *format, ...)
{
}
void Logger::log(FILE *output, const char *type, const char *format, va_list args)
{
}
#endif