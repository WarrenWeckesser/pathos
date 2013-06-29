
import unittest2 as unittest
from pathos.multiprocessing import ProcessingPool


class TestProcessingPool(unittest.TestCase):

    def test_map(self):
        # instantiate and configure the worker pool
        pool = ProcessingPool(nodes=4)

        result = map(pow, [1,2,3,4], [5,6,7,8]) 

        # do a blocking map on the chosen function
        pool_result = pool.map(pow, [1,2,3,4], [5,6,7,8])
        self.assertEqual(pool_result, result)

        # do a non-blocking map, then extract the result from the iterator
        pool_iter = pool.imap(pow, [1,2,3,4], [5,6,7,8])
        pool_result = list(pool_iter)
        self.assertEqual(pool_result, result)

        # do an asynchronous map, then get the results
        pool_queue = pool.amap(pow, [1,2,3,4], [5,6,7,8])
        pool_result = pool_queue.get()
        self.assertEqual(pool_result, result)


if __name__ == "__main__":
    unittest.main()
