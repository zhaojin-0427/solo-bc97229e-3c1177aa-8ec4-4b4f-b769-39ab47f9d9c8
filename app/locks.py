"""跨进程互斥锁（flock）。

多进程共用同一 DATA_DIR 时，鸟种字典替换全程只允许一个执行者：
- 排他锁在创建替换任务前获取，扫描 + 切换全程持有，任务终态后释放；
- 进程异常退出时内核自动释放 flock；
- 锁文件本身保留在 locks/ 下（flock 与文件存在性无关）。
"""
import contextlib
import fcntl
import os


class LockBusy(RuntimeError):
    """锁已被其他进程持有。"""


class ExclusiveLock:
    """可显式获取 / 释放的跨进程排他文件锁（允许在另一线程释放）。"""

    def __init__(self, lock_dir, name):
        os.makedirs(lock_dir, exist_ok=True)
        self.path = os.path.join(lock_dir, name + ".lock")
        self._fd = None

    def acquire(self):
        if self._fd is not None:
            return
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                raise LockBusy("已有替换任务在执行（锁被占用），同一时刻只允许一次替换")
        except BaseException:
            raise
        self._fd = fd

    def release(self):
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()


@contextlib.contextmanager
def file_lock(lock_dir, name):
    lock = ExclusiveLock(lock_dir, name)
    lock.acquire()
    try:
        yield lock.path
    finally:
        lock.release()


def is_locked(lock_dir, name):
    """非阻塞探测：True 表示当前有进程持锁。"""
    lock = ExclusiveLock(lock_dir, name)
    try:
        lock.acquire()
    except LockBusy:
        return True
    lock.release()
    return False
