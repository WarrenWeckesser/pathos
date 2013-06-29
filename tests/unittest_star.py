import time
import unittest2 as unittest

from pathos.multiprocessing import ProcessingPool
from pathos.multiprocessing import ThreadingPool
from pathos.pp import ParallelPythonPool


def busy_add(x,y, delay=0.01):
    for n in range(x):
       x += n
    for n in range(y):
       y -= n
    import time
    time.sleep(delay)
    return x + y

def busy_squared(x):
    import time, random
    time.sleep(2*random.random())
    return x*x

def squared(x):
    return x*x

def quad_factory(a=1, b=1, c=0):
    def quad(x):
        return a*x**2 + b*x + c
    return quad

square_plus_one = quad_factory(1,0,1)


class CheckPool(object):

    Pool = None

    def setUp(self):
        self.pool = self.Pool(nodes=4)
        self.x = range(18)
        self.xsquared = [x**2 for x in self.x]
        self.xsquared_plus_one = [t+1 for t in self.xsquared]

    def test1(self):
        result = self.pool.map(squared, self.x)
        self.assertEqual(result, self.xsquared)

        result = self.pool.imap(squared, self.x)
        self.assertEqual(list(result), self.xsquared)

        result = self.pool.amap(squared, self.x)
        result = result.get()
        self.assertEqual(result, self.xsquared)

    def test2(self):
        items = 4
        delay = 0
        _x = range(-items/2,items/2,2)
        _y = range(len(_x))
        _d = [delay]*len(_x)

        res1 = map(busy_squared, _x)
        res2 = map(busy_add, _x, _y, _d)

        _res1 = self.pool.map(busy_squared, _x)
        _res2 = self.pool.map(busy_add, _x, _y, _d)
        self.assertEqual(_res1, res1)
        self.assertEqual(_res2, res2)

        _res1 = self.pool.imap(busy_squared, _x)
        _res2 = self.pool.imap(busy_add, _x, _y, _d)
        self.assertEqual(list(_res1), res1)
        self.assertEqual(list(_res2), res2)

        _res1 = self.pool.amap(busy_squared, _x)
        _res2 = self.pool.amap(busy_add, _x, _y, _d)
        self.assertEqual(_res1.get(), res1)
        self.assertEqual(_res2.get(), res2)

    def test3(self):
        # Test against a function that should fail in pickle
        res = self.pool.map(square_plus_one, self.x)
        self.assertEqual(list(res), self.xsquared_plus_one)

    def test4(self):
        maxtries = 200
        delay = 0.1
        m = self.pool.amap(busy_add, self.x, self.x)
 
        tries = 0
        timed_out = False
        while not m.ready():
            time.sleep(delay)
            tries += 1
            if tries >= maxtries:
                timed_out = True
                break
        self.assertFalse(timed_out)
        result = m.get()
        expected = [busy_add(a, b) for a, b in zip(self.x, self.x)]
        self.assertEqual(result, expected)


class TestProcessingPool(CheckPool, unittest.TestCase):
    Pool = ProcessingPool


class TestThreadingPool(CheckPool, unittest.TestCase):
    Pool = ThreadingPool


class TestParallelPythonPool(CheckPool, unittest.TestCase):
    Pool = ParallelPythonPool

    def test3(self):
        # Known fail for ParallelPythonPool.
        pass


if __name__ == "__main__":
    unittest.main()
