
import time
import unittest2 as unittest
from pathos.multiprocessing import ProcessingPool
from pathos.python import PythonSerial
from pathos.pp import ParallelPythonPool



def my_add(x, y, delay=0.01):
    import time
    z = x + y
    time.sleep(delay)
    return z


class TestPools(unittest.TestCase):

    def setUp(self):
        n = 500
        self.x = range(n)
        self.y = range(-n//2, n//2)
        self.sum = [x + y for x, y in zip(self.x, self.y)]

    def test_python_serial(self):
        basic = PythonSerial()
        result = basic.map(my_add, self.x, self.y)
        self.assertEqual(list(result), self.sum)

    def test_parallel_python_pool(self):
        pool = ParallelPythonPool(4, servers=('localhost:5653',
                                              'localhost:2414'))
        result = pool.map(my_add, self.x, self.y)
        self.assertEqual(list(result), self.sum)

    def test_processing_pool(self):
        pool = ProcessingPool(4)
        result = pool.map(my_add, self.x, self.y)
        self.assertEqual(list(result), self.sum)

    def test_processing_pool_amap(self):
        pool = ProcessingPool(nodes=4)
        m = pool.amap(my_add, self.x, self.y)

        maxtries = 200
        delay = 0.1
        timed_out = False
        tries = 0
        while not m.ready():
            time.sleep(delay)
            tries += 1
            if tries >= maxtries:
                timed_out = True
                break
        self.assertFalse(timed_out)

        z = m.get()
        self.assertEqual(z, self.sum)


if __name__ == "__main__":
    unittest.main()
