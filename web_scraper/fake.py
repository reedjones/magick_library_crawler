__author__ = "reed@reedjones.me"

import contextvars
import random
import time
from functools import wraps

import trio

request_info = contextvars.ContextVar("request_info")

def get_tasks():
    for i in range(1, 200):
        if maybe():
            yield None
        yield i

def log(msg):
    # Read from task-local storage:
    request_tag = request_info.get()
    print(f"{request_tag}: {msg}")


class FakeError(Exception):
    pass


def tagged(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ts = time.time()
        request_info.set(f.__name__)
        result = f(*args, **kwargs)
        te = time.time()
        log(f"time: {te - ts} seconds...")
        return result

    return wrapper


async def sleepy(s=1, s2=10):
    await trio.sleep(random.choice(range(s, s2)))
    return


def maybe():
    return random.choice(range(10)) > 5


@tagged
async def upload(v):
    log(v)
    await sleepy()


@tagged
async def download(v):
    log(v)
    if maybe():
        await sleepy(40, 900)
    else:
        await sleepy()


@tagged
async def convert(v):
    log(v)
    if maybe():
        raise FakeError
    await sleepy(0, 100)



class Tracer(trio.abc.Instrument):
    def before_run(self):
        print("!!! run started")

    def _print_with_task(self, msg, task):
        # repr(task) is perhaps more useful than task.name in general,
        # but in context of a tutorial the extra noise is unhelpful.
        print(f"{msg}: {task.name}")

    def task_spawned(self, task):
        self._print_with_task("### new task spawned", task)

    def task_scheduled(self, task):
        self._print_with_task("### task scheduled", task)

    def before_task_step(self, task):
        self._print_with_task(">>> about to run one step of task", task)

    def after_task_step(self, task):
        self._print_with_task("<<< task step finished", task)

    def task_exited(self, task):
        self._print_with_task("### task exited", task)

    def before_io_wait(self, timeout):
        if timeout:
            print(f"### waiting for I/O for up to {timeout} seconds")
        else:
            print("### doing a quick check for I/O")
        self._sleep_time = trio.current_time()

    def after_io_wait(self, timeout):
        duration = trio.current_time() - self._sleep_time
        print(f"### finished I/O check (took {duration} seconds)")

    def after_run(self):
        print("!!! run finished")
