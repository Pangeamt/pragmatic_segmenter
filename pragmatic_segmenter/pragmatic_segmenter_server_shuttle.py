import aiohttp
import asyncio
import logging
import os


class PragmaticSegmenterServerShuttle:
    DEFAULT_DELAY_BEFORE_TEST = 0.2
    DEFAULT_TEST_TIMEOUT = 2
    DEFAULT_TEST_MAX_ATTEMPTS = 100
    DEFAULT_TEST_DELAY_BETWEEN_ATTEMPTS = 0.1
    DEFAULT_READ_PID_MAX_ATTEMPTS = 100
    DEFAULT_READ_PID_FILE_DELAY_BETWEEN_ATTEMPTS = 0.1

    def __init__(self,
                 rackup_config_path: str,
                 host: str,
                 port: int,
                 delay_before_test: float = DEFAULT_DELAY_BEFORE_TEST,
                 test_timeout: int = DEFAULT_TEST_TIMEOUT,
                 test_max_attempts: int = DEFAULT_TEST_MAX_ATTEMPTS,
                 test_delay_between_attempts: float = DEFAULT_TEST_DELAY_BETWEEN_ATTEMPTS,
                 read_pid_max_attempts: int = DEFAULT_READ_PID_MAX_ATTEMPTS,
                 read_pid_delay_between_attempts: float = DEFAULT_READ_PID_FILE_DELAY_BETWEEN_ATTEMPTS
                 ):

        self._rackup_config_path = rackup_config_path
        self._host = host
        self._port = port
        self._delay_before_test = delay_before_test
        self._test_timeout = test_timeout
        self._test_max_attempts = test_max_attempts
        self._test_delay_between_attempts = test_delay_between_attempts
        self._read_pid_max_attempts = read_pid_max_attempts
        self._read_pid_delay_between_attempts = read_pid_delay_between_attempts

        self._url = 'http://'+ host + ':' + str(port) + '/segment'
        self._process = None

        # Log
        self._logger = logging.getLogger("Segmenter")
        self._logger_monitor = logging.getLogger("Segmenter stdout")

        # Events
        self._start_event = _EventWithStatus()
        self._segmenter_server_pid_retrieved_event = _EventWithStatus()
        self._after_test_event = _EventWithStatus()
        self._fatal_error_event = asyncio.Event()

        # Tasks
        self._run_task = None
        self._monitor_task = None
        self._handle_process_error_task = None

        # Segment server pid file
        self._segmenter_server_pid_file = os.path.abspath("segmenter_server.pid")
        self._segmenter_server_pid = None

    async def start(self) -> None:
        """Run the segmenter server
        """

        # Create a task to run the segmenter
        self._run_task = asyncio.create_task(self._run())

        # Wait for some events to be completed...
        await self._segmenter_server_pid_retrieved_event.wait()
        if not self._segmenter_server_pid_retrieved_event.status:
            raise SegmenterStartError('Retreiving server pid failed')

        await self._after_test_event.wait()
        if not self._after_test_event.status:
            raise SegmenterStartError('Test failed')

    async def _run(self):
        # Segmenter server pid initialization
        self._segmenter_server_pid = None
        if os.path.isfile(self._segmenter_server_pid_file):
            os.remove(self._segmenter_server_pid_file)

        # Create cmd
        cmd = f"rackup -p {self._port} --host {self._host} --pid \"{self._segmenter_server_pid_file}\" {self._rackup_config_path}"
        self._logger.info(f"Segmenter cmd: {cmd}")

        # Create process
        self._process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        # Create task to monitor stdout
        self._monitor_task = asyncio.create_task(self._monitor())

        # Get the pid of the segmentation server
        for _ in range(self._read_pid_max_attempts):
            if os.path.isfile(self._segmenter_server_pid_file):
                with open(self._segmenter_server_pid_file, 'r') as f:
                    self._segmenter_server_pid = int(f.read())
                    self._segmenter_server_pid_retrieved_event.set_status(True)
                    break
            await asyncio.sleep(self._read_pid_delay_between_attempts)

        if self._segmenter_server_pid is None:
            self._segmenter_server_pid_retrieved_event.set_status(False)
            return

        # Create task to handle process error
        self._handle_process_error_task = asyncio.create_task(self._handle_process_error())

        # Wait a bit before starting the test
        await asyncio.sleep(self._delay_before_test)

        # Test
        ok = False
        for i in range(self._test_max_attempts):
            try:
                timeout = aiohttp.ClientTimeout(total=self._test_timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(self._url, json={
                        "lang": "en",
                        "texts": ["Hello"]
                    }) as response:
                        if response.status == 200:
                            ok = True
                            break
                        else:
                            error = f'Segmenter error {response.status}'
                            msg = await response.text()
                            if msg:
                                error += f': {msg}'
                            self._logger.info(response)
            except Exception as e:
                self._logger.info(type(e).__name__ + " " + str(e))

            await asyncio.sleep(self._test_delay_between_attempts)

        if not ok:
            self._logger.fatal(f"Segmenter test failed after {self._test_max_attempts} attempts")
            self._after_test_event.set_status(False)
            return

        self._after_test_event.set_status(True)

    async def stop(self):
        """Stop the segmenter
        """
        try:
            self._monitor_task.cancel()
        except Exception as e:
            pass

        if self._segmenter_server_pid is not None:
            logging.debug(f"killing segmenter server process {self._segmenter_server_pid}")
            os.kill(self._segmenter_server_pid, 9)
            self._segmenter_server_pid = None

        if self._process is not None and self._process.returncode is None:
            try:
                logging.debug(f"killing segmenter shell process {self._process.pid}")
                self._process.kill()
            except Exception as e:
                print(e)

    async def _handle_process_error(self)->None:
        """Handle process error
        """

        await self._process.wait()
        await self.stop()

    async def _monitor(self) -> None:
        """monitor segmenter process stderr
        """

        while True:
            line = await self._process.stderr.readline()
            line = line.decode('ascii')
            if line == '':
                # If process finishes an empty line is received. We have to break the loop
                break
            self._logger_monitor.debug(line.rstrip())


class _EventWithStatus(asyncio.Event):
    def __init__(self):
        super().__init__()
        self._status = None

    def set_status(self, status):
        self._status = status
        super().set()

    def get_status(self):
        return self._status
    status = property(get_status, set_status)


class SegmenterConnectionError(Exception):
    def __init__(self):
        msg = "Segmenter connection error"
        super(SegmenterConnectionError, self).__init__(msg)


class SegmenterPayloadError(Exception):
    def __init__(self):
        msg = "Segmenter payload error"
        super(SegmenterPayloadError, self).__init__(msg)


class SegmenterError(Exception):
    def __init__(self):
        msg = "Segmenter error"
        super(SegmenterError, self).__init__(msg)


class SegmenterStartError(Exception):
    def __init__(self, msg):
        super().__init__(f"Segmenter start error. {msg}")
