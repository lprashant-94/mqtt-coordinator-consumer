import os
import random
import shutil
import tempfile
from Queue import Empty, Queue
from StringIO import StringIO

from nose.tools import assert_equal
from pqueue import JournaledPersistentQueue, PersistentQueue, QueueError


def assert_raises(func, exc_type, str_contains=None, repr_contains=None):
    try:
        func()
    except exc_type as e:
        if str_contains is not None and str_contains not in str(e):
            raise AssertionError("%s raised, but %r does not contain %r"
                                 % (exc_type, str(e), str_contains))
        if repr_contains is not None and repr_contains not in repr(e):
            raise AssertionError("%s raised, but %r does not contain %r"
                                 % (exc_type, repr(e), repr_contains))
        return e
    else:
        raise AssertionError("%s not raised" % (exc_type, ))


def run_blackbox_queue_verification(load_test_queue, test_qsize=False,
                                    operation_count=400):
    performed = []

    def perform_operation(queue, operation, arg, is_reference, **kwargs):
        args = operation == "put_nowait" and (arg, ) or ()
        if is_reference:
            performed.append((operation, args))

        if operation == "reload":
            if is_reference:
                return
            else:
                test_queue[0].close()
                test_queue[0] = load_test_queue()
                return

        if operation == "qsize":
            return queue.qsize()

        try:
            return getattr(queue, operation)(*args, **kwargs)
        except Empty:
            return Empty

    base_operations = ["peek_nowait", "get_nowait", "put_nowait", "reload"]
    if test_qsize:
        base_operations += ["qsize", "empty"]

    reference_queue = Queue()

    def peek_nowait(**kwargs):
        if not reference_queue.queue:
            raise Empty()
        return reference_queue.queue[0]
    reference_queue.peek_nowait = peek_nowait

    test_queue = [load_test_queue()]

    try:
        biases = ["grow", "shrink", "grow", "shrink"]
        operation_count = operation_count / len(biases)
        for bias in biases:
            if bias == "grow":
                operations = base_operations + ["put_nowait"]
            else:
                operations = base_operations + ["get_nowait"]

            for num in xrange(operation_count):
                operation = random.choice(operations)
                expected = perform_operation(reference_queue, operation, num, True)
                actual = perform_operation(test_queue[0], operation, num, False)
                assert_equal(expected, actual)
    except:
        print("operations performed:")
        print(performed)
        raise


class TempdirTestBase(object):
    def setup(self):
        self.dirname = tempfile.mkdtemp(prefix="pqueue-test-")
        self.path = lambda *a: os.path.join(self.dirname, *a)

    def teardown(self):
        shutil.rmtree(self.dirname)


class TestJournaledPersistentQueueFilesystemThings(TempdirTestBase):
    def queue(self, filename=None, create=True):
        return JournaledPersistentQueue(
            filename or self.path("testfile"),
            create=create
        )

    def test_create_file(self):
        queue = self.queue()
        assert_equal(queue.qsize(), 0)
        assert_equal(queue.empty(), True)

    def test_get_empty(self):
        queue = self.queue()
        assert_raises(queue.peek_nowait, Empty)
        assert_raises(queue.get_nowait, Empty)

    def test_put(self):
        queue = self.queue()
        queue.put_nowait(1)
        queue.put_nowait(2)
        assert_equal(queue.peek_nowait(), 1)
        assert_equal(queue.get_nowait(), 1)
        assert_equal(queue.get_nowait(), 2)

    def test_blackbox(self):
        self.queue(create=True).close()
        run_blackbox_queue_verification(lambda: self.queue(create=False),
                                        test_qsize=True)

    def test_create_existing_file(self):
        filename = self.path("testfile")
        open(filename, "w").close()
        assert_raises(lambda: self.queue(filename, create=True),
                      QueueError, str_contains=filename)

    def test_load_existing_file(self):
        q1 = self.queue()
        q1.put_nowait(1)
        q1.put_nowait(2)
        q1.put_nowait(3)
        q1.close()

        q2 = self.queue(q1.filename, create=False)
        assert_equal(q2.get_nowait(), 1)
        assert_equal(q2.get_nowait(), 2)
        assert_equal(q2.get_nowait(), 3)
        assert_raises(q2.get_nowait, Empty)


class JournaledPersistentQueueWithFakeOpen(JournaledPersistentQueue):
    def __init__(self, fake_file, *args, **kwargs):
        self.fake_file = fake_file
        super(JournaledPersistentQueueWithFakeOpen, self).__init__(
            "/does/not/exist", *args, **kwargs
        )

    def _open(self, create):
        return self.fake_file


class TestJournaledPersistentQueueLogicThings(object):
    def queue(self, initial_data="", create=False):
        fake_file = StringIO(initial_data)
        fake_file.seek(0)
        queue = JournaledPersistentQueueWithFakeOpen(fake_file, create=create)
        return fake_file, queue

    def get_file_contents(self, items, active=True):
        data, queue = self.queue(create=True)
        for item in items:
            queue.put_nowait(item)
        if not active:
            queue.get_nowait()
        data.seek(0)
        return data.read()

    def test_get_file_contents(self):
        _, queue = self.queue(self.get_file_contents(["foo", "bar"]))
        assert_equal(queue.get_nowait(), "foo")
        assert_equal(queue.get_nowait(), "bar")
        assert_raises(queue.get_nowait, Empty)

    def test_data_truncated(self):
        data = self.get_file_contents(["foo", "bar", "ohno!"])[:-3]
        _, queue = self.queue(data)
        assert_equal(queue.get_nowait(), "foo")
        assert_equal(queue.get_nowait(), "bar")
        assert_raises(queue.get_nowait, Empty)

    def test_data_truncated_after_load(self):
        file, queue = self.queue()
        queue.put_nowait("foo")
        queue.put_nowait("ohno!")
        old_position = file.tell()
        file.seek(-5, 2)
        file.truncate()
        file.seek(old_position)
        assert_equal(queue.get_nowait(), "foo")
        assert_raises(queue.get_nowait, Empty)

    def test_data_checksum_fail_after_load(self):
        file, queue = self.queue()
        queue.put_nowait("foo")
        queue.put_nowait("ohno!")
        old_position = file.tell()
        file.seek(-5, 2)
        file.write("x")
        file.seek(old_position)
        assert_equal(queue.get_nowait(), "foo")
        assert_raises(queue.get_nowait, Empty)
        queue.put_nowait("bar")
        assert_equal(queue.get_nowait(), "bar")

    def test_fuzzing(self):
        file, queue = self.queue()
        queue.put_nowait("foo")
        queue.put_nowait("bar")
        queue.get_nowait()
        queue.put_nowait("baz")
        queue.get_nowait()
        file.seek(0)
        # A simple fuzzing test. Walks over the file, flipping the least
        # significant bit of each byte, then loads this "bad" file and performs
        # some simple operations to make sure that no exceptions are raised.
        bytes = [ord(x) for x in file.read()]
        for idx, _ in enumerate(bytes):
            bytes[idx] ^= 0x01
            _, queue = self.queue("".join(map(chr, bytes)))
            queue.put_nowait("test")
            for _ in range(queue.qsize()):
                queue.get_nowait()
            queue.put_nowait("test")
            assert_equal(queue.get_nowait(), "test")
            bytes[idx] ^= 0x01


class TestPersistentQueue(TempdirTestBase):
    def test_blackbox(self):
        def mk_queue(): return PersistentQueue(self.dirname, max_filesize=1000)
        run_blackbox_queue_verification(mk_queue)
