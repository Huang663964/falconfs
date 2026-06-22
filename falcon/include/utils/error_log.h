/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_ERROR_LOG_H
#define FALCON_ERROR_LOG_H

#include "postgres.h"

#include "utils/elog.h"

#include "utils/error_code.h"

#include <stdio.h>
#include <unistd.h>

#define FALCON_ELOG_ERROR(errorCode, errorMsg) \
    elog(ERROR, "%c %s | %s:%d", ((char)errorCode + 64), errorMsg, __FILE__, __LINE__)
#define FALCON_ELOG_ERROR_EXTENDED(errorCode, errorFormat, ...) \
    elog(ERROR, "%c " errorFormat, ((char)errorCode + 64), __VA_ARGS__)
#define FALCON_ELOG_WARNING(errorCode, errorMsg) \
    elog(WARNING, "%c %s | %s:%d", ((char)errorCode + 64), errorMsg, __FILE__, __LINE__)
#define FALCON_ELOG_WARNING_EXTENDED(errorCode, errorFormat, ...) \
    elog(WARNING, "%c " errorFormat, ((char)errorCode + 64), __VA_ARGS__)
#define FALCON_RAW_WARNING_EXTENDED(errorCode, errorFormat, ...)                                      \
    do {                                                                                               \
        char falconRawLogBuffer[1024];                                                                 \
        int falconRawLogLength = snprintf(falconRawLogBuffer,                                          \
                                          sizeof(falconRawLogBuffer),                                  \
                                          "[WARNING] [FALCON_METADB] %c " errorFormat "\n",          \
                                          ((char)(errorCode) + 64),                                    \
                                          __VA_ARGS__);                                                \
        if (falconRawLogLength > 0) {                                                                  \
            size_t falconRawLogWriteLength = falconRawLogLength < (int)sizeof(falconRawLogBuffer)      \
                                                  ? (size_t)falconRawLogLength                         \
                                                  : sizeof(falconRawLogBuffer) - 1;                    \
            (void)write(STDERR_FILENO, falconRawLogBuffer, falconRawLogWriteLength);                   \
        }                                                                                              \
    } while (0)

#endif
