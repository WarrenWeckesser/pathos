
import unittest2 as unittest
from dill import source
from pathos.pp import ParallelPythonPool

from math import sin

f = lambda x:x+1

def g(x):
    return x+2


class MyTest(unittest.TestCase):

    def test1(self):
        for func in [g, f, abs, sin]:
            _obj = source._wrap(func)
            self.assertEqual(_obj(1.57), func(1.57))
            src = source.getsource(func, alias='_f')
            exec src in globals(), locals()
            self.assertEqual(_f(1.57), func(1.57))
            name = source._get_name(func)
            self.assertTrue(name == func.__name__ or src.split("=",1)[0].strip())

    def test2(self):
        for func in [g, f, abs, sin]:
            p = ParallelPythonPool(2)
            x = [1,2,3]
            assert map(func, x) == p.map(func, x)


if __name__ == '__main__':
    unittest.main()
