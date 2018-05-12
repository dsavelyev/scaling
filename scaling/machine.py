import logging
import os
import os.path
import shlex
import stat
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

import paramiko
import toolz


_logger = logging.getLogger(__name__)


class LocalMachine:
    def run_command(self, args, stdin=b''):
        result = subprocess.run(
            args, stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return (result.stdout, result.stderr, result.returncode)

    def get_file(self, path, text=True):
        mode = 'r' if text else 'rb'
        with open(path, mode) as f:
            return f.read()

    def put_file(self, path, data):
        if isinstance(data, str):
            mode = 'w'
        elif isinstance(data, bytes):
            mode = 'wb'
        else:
            raise TypeError('expected str or bytes')

        with open(path, mode) as f:
            f.write(data)

    def mkdir(self, path):
        if not os.path.exists(path):
            os.mkdir(path, 0o755)
            return False
        elif not os.path.isdir(path):
            raise FileExistsError
        else:
            return True

    def mkdtemp(self, prefix):
        dirname, basename = os.path.split(prefix)
        return tempfile.mkdtemp(dir=dirname, prefix=basename)

    def list_files(self, dirname):
        raise NotImplementedError


class PasswordRequiredException(Exception):
    pass


class SSHMachine:
    def __init__(self, host, port=22, username=None, pkey=None, password=None,
                 passphrase=None):
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.WarningPolicy())

        try:
            self.client.connect(
                host,
                port=port,
                username=username,
                pkey=pkey,
                password=password,
                passphrase=passphrase)
        except paramiko.PasswordRequiredException as e:
            raise PasswordRequiredException(str(e))

        self.sftp_chan = self.client.get_transport().open_session()
        self.sftp_chan.invoke_subsystem('sftp')
        self.sftp_client = paramiko.SFTPClient(self.sftp_chan)

        self.lock = threading.Lock()

        self.closed = False

    def _raise_if_closed(self):
        if self.closed:
            raise ValueError('this SSHMachine is closed')

    def run_command(self, args, stdin=b''):
        with self.lock:
            self._raise_if_closed()

            _logger.debug(args)
            _logger.debug(stdin)
            cmd = ' '.join(map(shlex.quote, args))

            with self.client.get_transport().open_session() as chan:
                chan.exec_command(cmd)

                def read_thread(method):
                    s = bytearray()

                    while True:
                        temp = method(4096)
                        if temp:
                            s += temp
                        else:
                            break

                    return bytes(s)

                with ThreadPoolExecutor(max_workers=2) as executor:
                    out_future = executor.submit(read_thread, chan.recv)
                    err_future = executor.submit(read_thread, chan.recv_stderr)

                    chan.sendall(stdin)
                    chan.shutdown_write()

                out, err = out_future.result(), err_future.result()

                exitcode = chan.recv_exit_status()
                _logger.debug(f'out {out}, err {err}, exitcode {exitcode}')
                return out, err, exitcode

    # FIXME: newline handling

    def get_file(self, path, text=True):
        with self.lock:
            self._raise_if_closed()

            with self.sftp_client.file(path) as f:
                result = f.read()
                if text:
                    result = result.decode('utf-8')
                return result

    def put_file(self, path, data):
        with self.lock:
            self._raise_if_closed()

            if isinstance(data, str):
                data = data.encode('utf-8')
            with self.sftp_client.file(path, 'w') as f:
                f.write(data)

    def mkdir(self, path):
        with self.lock:
            self._raise_if_closed()

            try:
                st = self.sftp_client.stat(path)
            except FileNotFoundError:
                self.sftp_client.mkdir(path, 0o755)
                return False
            else:
                if not stat.S_ISDIR(st.st_mode):
                    raise FileExistsError
                return True

    def mkdtemp(self, prefix):
        _logger.debug(f'mkdtemp {prefix}')
        out, _, exitcode = self.run_command(
            ['mktemp', '-d', prefix + '.XXXXXX'])
        if exitcode != 0:
            raise OSError
        return out.decode('utf-8').strip('\r\n')

    def list_files(self, dirname):
        with self.lock:
            self._raise_if_closed()

            it = self.sftp_client.listdir_iter(dirname)
            for entry in it:
                yield entry.filename

    def close(self):
        self._raise_if_closed()

        self.client.close()
        self.closed = True

    def __enter__(self):
        self._raise_if_closed()
        return self

    def __exit__(self, *args):
        self.close()
