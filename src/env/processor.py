import threading
from queue import Queue, Empty
import time

import logging
logger = logging.getLogger(__name__)

class Processor(threading.Thread):

    _mutex = threading.RLock()      # Mutex for single-threaded mode 
    _single_threaded = False        # Global threading mode flag, default is free-threaded
    _running = True                 # Global running flag

    def __init__(self, name=None, event_q=None):

        super().__init__(args=(name,), daemon=True) # Ensure thread exits when main program exits

        self._event_q = event_q if event_q else Queue()

        self._event = None
        self._event_timestamp = None

    @staticmethod
    def stop_all():
        Processor._running = False

    @staticmethod
    def single_thread():
        Processor._mutex.acquire() # Need to own mutex to change threading mode
        if Processor._single_threaded: # Already in single-threaded mode
            Processor._mutex.release() # Release nested mutex (we only need to own it once)
        else:
            Processor._single_threaded = True # Set mode to single-threaded, and retain ownership of the mutex

    @staticmethod
    def free_thread():
        Processor._mutex.acquire() # Need to own mutex to change threading mode
        if Processor._single_threaded:
            Processor._single_threaded = False # Switch mode to free-threaded
            Processor._mutex.release() # Release nested mutex
        Processor._mutex.release() # Release overall ownership of mutex

    def put_queue(self, event_q: Queue):
        self._event_q = event_q

    def get_queue(self) -> Queue:
        return self._event_q

    def get_current_event(self):
        return self._event

    def get_current_event_processing_time(self):
        return time.time() - self._event_timestamp if self._event_timestamp else None

    def run(self):
        """ Thread run method to process events from the queue 
            in either single-threaded or free-threaded mode.
            In single-threaded mode, only one processor can process events at a time.
            In free-threaded mode, multiple processors can process events concurrently.
        """
        logger.debug(f"Processor {self.name} started running")

        while Processor._running:

            acquired_mutex = False

            # If in single-threaded mode, other threads will block here until mutex is free
            if Processor._single_threaded:
                Processor._mutex.acquire()
                acquired_mutex = True

            # Check if we should stop running the processor / thread
            if not Processor._running:
                if acquired_mutex:
                    Processor._mutex.release()
                    self.free_thread()  # Ensure other threads can unblock to stop running

                logger.debug(f"Processor {self.name} received stop signal, exiting")
                break

            try:
                self._event = self._event_q.get(timeout=1)  # Wait for an event for up to 1 second
                self._event_timestamp = time.perf_counter()

                try:
                    self.process_event(self._event)
                finally:
                    self._event_q.task_done()

            except Empty:
                pass
            except Exception as e:
                logger.exception(f"Processor: Exception occurred while processing event {self._event} in processor {self.name}: {e}")
            finally:
                if acquired_mutex:
                    Processor._mutex.release()
                
                self._event = None
                self._event_timestamp = None

        self.join()  # Wait for the thread to finish

    def process_event(self, event) -> bool:
        """ Processes an event from the queue.
            Subclasses must implement this method.
            : returns: True if event was processed, False if event was ignored.
        """
        raise NotImplementedError("Subclasses must implement process_event method")

class TestProcessor(Processor):

    def __init__(self, nr:int, event_q=None):
        super().__init__(event_q=event_q)
        
        self._nr = nr

    def process_event(self, event) -> bool:

        if event > 20 and event < 40:
            Processor.single_thread()
            print(f"s:", end='', flush=True)
        else:
            Processor.free_thread()

        print(f"{self._nr}", end='', flush=True)
        print(f", ", end='', flush=True)
        print(f"{event}", flush=True)
        
        return True

if __name__ == "__main__":

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,  # Or DEBUG for more verbosity
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger(__name__)

    q = Queue()

    test1 = TestProcessor(1, event_q=q)
    test2 = TestProcessor(2, event_q=q)
    test3 = TestProcessor(3, event_q=q)
    test4 = TestProcessor(4, event_q=q)

    test1.start()
    test2.start()
    test3.start()
    test4.start()

    for i in range(400):
        q.put(i)

    time.sleep(5)  # Allow some time for processing to complete

    Processor.stop_all()
