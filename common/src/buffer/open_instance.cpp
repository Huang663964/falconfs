/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "buffer/open_instance.h"

#include <unistd.h>

OpenInstance::~OpenInstance()
{
    readStream.StopPushThreaded();
    readStream.WaitPushEnded();
    ReleaseCacheLock();
}

void OpenInstance::LockOpenInstance() { fileMutex.lock(); }
void OpenInstance::UnlockOpenInstance() { fileMutex.unlock(); }

void OpenInstance::SetCacheLockFd(int fd)
{
    if (cacheLockFd == fd) {
        return;
    }
    ReleaseCacheLock();
    cacheLockFd = fd;
}

bool OpenInstance::HasCacheLock() const { return cacheLockFd >= 0; }

void OpenInstance::ReleaseCacheLock()
{
    if (cacheLockFd >= 0) {
        close(cacheLockFd);
        cacheLockFd = -1;
    }
}
