from contextlib import redirect_stdout
from email.utils import formatdate
import hashlib
import http.client
import io
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, call, patch
import urllib.error

from test_tools import load


class FormatterResponse(io.BytesIO):
    def __init__(self, data, failure=None):
        super().__init__(data)
        self.failure = failure

    def read(self, *arguments, **options):
        if self.failure is not None:
            raise self.failure
        return super().read(*arguments, **options)


class FailingWrite:
    def __init__(self, stream):
        self.stream = stream

    def __getattr__(self, name):
        return getattr(self.stream, name)

    def __enter__(self):
        self.stream.__enter__()
        return self

    def __exit__(self, *arguments):
        return self.stream.__exit__(*arguments)

    def write(self, data):
        self.stream.write(data[:3])
        raise OSError('temporary write failed')


class FormatterDownloadTest(unittest.TestCase):
    DATA = b'synthetic formatter bytes\x00\xff'
    NOW = 1700000000.75

    def setUp(self):
        self.formatter = load('format-java')
        self.pinned_sha256 = self.formatter.SHA256
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.stderr = io.StringIO()
        self.replace_attribute(self.formatter, 'ROOT', self.root)
        self.replace_attribute(self.formatter, 'SHA256', hashlib.sha256(self.DATA).hexdigest())
        self.urlopen = self.replace_attribute(self.formatter.urllib.request, 'urlopen',
                                             side_effect=AssertionError('unexpected network request'))
        self.sleep = self.replace_attribute(self.formatter.time, 'sleep')
        self.replace_attribute(self.formatter.time, 'time', return_value=self.NOW)
        self.java = self.replace_attribute(self.formatter.subprocess, 'run')
        self.replace_attribute(sys, 'stderr', self.stderr)

    def replace_attribute(self, target, name, *arguments, **options):
        patcher = patch.object(target, name, *arguments, **options)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    @property
    def jar(self):
        return self.root / '.tools' / ('google-java-format-' + self.formatter.VERSION + '.jar')

    def cache(self, data=DATA):
        self.jar.parent.mkdir(exist_ok=True)
        self.jar.write_bytes(data)

    def response(self, failure=None, data=DATA):
        response = FormatterResponse(data, failure)
        self.addCleanup(response.close)
        return response

    def http_error(self, status, advice=None):
        body = io.BytesIO(b'synthetic HTTP error body')
        self.addCleanup(body.close)
        headers = {} if advice is None else {'Retry-After': advice}
        error = urllib.error.HTTPError('https://example.invalid/formatter.jar', status,
                                       'synthetic HTTP error', headers, body)
        return error, body

    def assert_no_download_files(self):
        self.assertFalse(self.jar.exists())
        if self.jar.parent.exists():
            self.assertEqual([], list(self.jar.parent.iterdir()))

    def run_main(self, *arguments):
        with patch('sys.argv', ['format-java.py', *arguments]):
            return self.formatter.main()

    def test_pinned_formatter_coordinates_are_unchanged(self):
        self.assertEqual('1.24.0', self.formatter.VERSION)
        self.assertEqual('812f805f58112460edf01bf202a8e61d0fd1f35c0d4fabd54220640776ec57a1',
                         self.pinned_sha256)
        response = self.response()
        self.urlopen.side_effect = [response]
        self.assertEqual(self.jar, self.formatter.ensure_formatter())
        self.urlopen.assert_called_once_with(
            'https://repo.maven.apache.org/maven2/com/google/googlejavaformat/'
            'google-java-format/1.24.0/google-java-format-1.24.0-all-deps.jar', timeout=60)
        self.assertEqual(self.DATA, self.jar.read_bytes())
        self.assertTrue(response.closed)
        self.sleep.assert_not_called()

    def test_each_retryable_http_status_closes_before_sleep_and_then_succeeds(self):
        for status in (429, 500, 502, 503, 504):
            with self.subTest(status=status):
                error, body = self.http_error(status)
                response = self.response()
                self.urlopen.reset_mock(side_effect=True)
                self.urlopen.side_effect = [error, response]
                self.sleep.reset_mock()
                self.sleep.side_effect = lambda delay: self.assertTrue(body.closed)
                self.assertEqual(self.jar, self.formatter.ensure_formatter())
                self.assertEqual(2, self.urlopen.call_count)
                self.sleep.assert_called_once_with(10)
                self.assertTrue(response.closed)
                self.assertEqual(self.DATA, self.jar.read_bytes())
                self.jar.unlink()

    def test_transport_failures_retry_during_open_and_response_read(self):
        failures = (('timeout', lambda: TimeoutError('timed out')),
                    ('socket timeout', lambda: socket.timeout('timed out')),
                    ('connection', lambda: ConnectionError('connection lost')),
                    ('incomplete read', lambda: http.client.IncompleteRead(b'partial', 20)))
        for name, factory in failures:
            for wrapped in (False, True):
                for reading in (False, True):
                    with self.subTest(failure=name, wrapped=wrapped, reading=reading):
                        failure = factory()
                        if wrapped:
                            failure = urllib.error.URLError(failure)
                        first = self.response(failure=failure) if reading else failure
                        response = self.response()
                        self.urlopen.reset_mock(side_effect=True)
                        self.urlopen.side_effect = [first, response]
                        self.sleep.reset_mock(side_effect=True)
                        if reading:
                            self.sleep.side_effect = lambda delay: self.assertTrue(first.closed)
                        self.assertEqual(self.jar, self.formatter.ensure_formatter())
                        self.assertEqual(2, self.urlopen.call_count)
                        self.sleep.assert_called_once_with(10)
                        self.assertTrue(response.closed)
                        self.jar.unlink()

    def test_exhaustion_has_four_attempts_and_no_final_sleep(self):
        for advice, delays in ((None, [10, 20, 40]), ('60', [60, 60, 60])):
            with self.subTest(advice=advice):
                failures = [self.http_error(503, advice) for _ in range(4)]
                self.urlopen.reset_mock(side_effect=True)
                self.urlopen.side_effect = [error for error, body in failures]
                self.sleep.reset_mock()

                def sleep(delay):
                    for error, body in failures[:self.urlopen.call_count]:
                        self.assertTrue(body.closed)

                self.sleep.side_effect = sleep
                with self.assertRaises(RuntimeError) as raised:
                    self.formatter.ensure_formatter()
                self.assertIs(failures[-1][0], raised.exception.__cause__)
                self.assertRegex(str(raised.exception), r'Google Java Format.*4')
                self.assertEqual(4, self.urlopen.call_count)
                self.assertEqual([call(delay) for delay in delays], self.sleep.call_args_list)
                self.assertLessEqual(sum(delays), 180)
                self.assertTrue(all(body.closed for error, body in failures))
                self.assert_no_download_files()

    def test_permanent_http_errors_are_closed_without_retry(self):
        for status in (404, 501):
            with self.subTest(status=status):
                error, body = self.http_error(status, '60')
                self.urlopen.reset_mock(side_effect=True)
                self.urlopen.side_effect = error
                with self.assertRaises(RuntimeError) as raised:
                    self.formatter.ensure_formatter()
                self.assertIs(error, raised.exception.__cause__)
                self.assertTrue(body.closed)
                self.urlopen.assert_called_once()
                self.sleep.assert_not_called()
                self.assert_no_download_files()

    def test_dns_tls_and_other_url_errors_do_not_retry(self):
        failures = (socket.gaierror(-2, 'name unavailable'), ssl.SSLError('TLS failed'),
                    urllib.error.URLError(socket.gaierror(-2, 'name unavailable')),
                    urllib.error.URLError(ssl.SSLError('TLS failed')),
                    urllib.error.URLError('unclassified network failure'))
        for failure in failures:
            with self.subTest(failure=repr(failure)):
                self.urlopen.reset_mock(side_effect=True)
                self.urlopen.side_effect = failure
                with self.assertRaises(RuntimeError) as raised:
                    self.formatter.ensure_formatter()
                self.assertIs(failure, raised.exception.__cause__)
                self.urlopen.assert_called_once()
                self.sleep.assert_not_called()
                self.assert_no_download_files()

    def test_retry_after_uses_advice_without_shortening_fallback(self):
        future = formatdate(1700000051, usegmt=True)
        past = formatdate(1699999990, usegmt=True)
        cases = ((None, 10), ('0', 10), ('25', 25), ('60', 60), ('000060', 60),
                 ('0' * 5000 + '60', 60),
                 ('-1', 10), ('+5', 10), ('1.5', 10), ('tomorrow', 10), ('', 10),
                 ('١٠', 10), ('６０', 10), (future, 50.25), (past, 10))
        for advice, expected in cases:
            with self.subTest(advice=advice):
                error, body = self.http_error(429, advice)
                response = self.response()
                self.urlopen.reset_mock(side_effect=True)
                self.urlopen.side_effect = [error, response]
                self.sleep.reset_mock()
                self.sleep.side_effect = lambda delay: self.assertTrue(body.closed)
                self.assertEqual(self.jar, self.formatter.ensure_formatter())
                self.sleep.assert_called_once_with(expected)
                self.assertEqual(2, self.urlopen.call_count)
                self.assertTrue(response.closed)
                self.jar.unlink()

    def test_small_advice_does_not_shorten_later_fallbacks(self):
        failures = [self.http_error(503, '15') for _ in range(3)]
        response = self.response()
        self.urlopen.side_effect = [error for error, body in failures] + [response]
        self.assertEqual(self.jar, self.formatter.ensure_formatter())
        self.assertEqual([call(15), call(20), call(40)], self.sleep.call_args_list)
        self.assertTrue(all(body.closed for error, body in failures))
        self.assertTrue(response.closed)

    def test_retry_after_over_sixty_seconds_fails_without_sleep_or_retry(self):
        future = formatdate(1700000061, usegmt=True)
        for advice in ('61', '000061', '9' * 5000, future):
            with self.subTest(advice=advice[:80]):
                error, body = self.http_error(429, advice)
                self.urlopen.reset_mock(side_effect=True)
                self.urlopen.side_effect = error
                with self.assertRaises(RuntimeError):
                    self.formatter.ensure_formatter()
                self.assertTrue(body.closed)
                self.urlopen.assert_called_once()
                self.sleep.assert_not_called()
                self.assert_no_download_files()

    def test_valid_cache_is_hashed_on_each_use_without_network(self):
        self.cache()
        self.assertEqual(self.jar, self.formatter.ensure_formatter())
        self.jar.write_bytes(self.DATA + b'changed')
        with self.assertRaises(RuntimeError):
            self.formatter.ensure_formatter()
        self.assertEqual(self.DATA + b'changed', self.jar.read_bytes())
        self.urlopen.assert_not_called()
        self.sleep.assert_not_called()

    def test_corrupt_cache_fails_main_without_modifying_it_or_running_java(self):
        self.cache(b'corrupt existing formatter')
        with self.assertRaises(SystemExit) as raised:
            self.run_main('--check')
        self.assertEqual(1, raised.exception.code)
        self.assertEqual(b'corrupt existing formatter', self.jar.read_bytes())
        self.assertEqual([self.jar], list(self.jar.parent.iterdir()))
        self.urlopen.assert_not_called()
        self.sleep.assert_not_called()
        self.java.assert_not_called()
        self.assertTrue(self.stderr.getvalue())

    def test_bad_download_checksum_does_not_publish_or_run_java(self):
        response = self.response(data=b'incorrect downloaded formatter')
        self.urlopen.side_effect = [response]
        with self.assertRaises(SystemExit) as raised:
            self.run_main('--check')
        self.assertEqual(1, raised.exception.code)
        self.assertTrue(response.closed)
        self.urlopen.assert_called_once()
        self.sleep.assert_not_called()
        self.java.assert_not_called()
        self.assert_no_download_files()

    def test_cache_read_failure_preserves_the_file_and_does_not_retry(self):
        self.cache()
        with patch.object(Path, 'read_bytes', side_effect=PermissionError('cache read denied')):
            with self.assertRaises(PermissionError):
                self.formatter.ensure_formatter()
        self.assertEqual(self.DATA, self.jar.read_bytes())
        self.urlopen.assert_not_called()
        self.sleep.assert_not_called()

    def test_response_read_failure_closes_without_retry_or_publication(self):
        response = self.response(failure=OSError('response read failed'))
        self.urlopen.side_effect = [response]
        with self.assertRaises(RuntimeError) as raised:
            self.formatter.ensure_formatter()
        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertTrue(response.closed)
        self.urlopen.assert_called_once()
        self.sleep.assert_not_called()
        self.assert_no_download_files()

    def test_publication_closes_unique_temporary_file_before_replace(self):
        self.jar.parent.mkdir()
        foreign = self.jar.parent / '.google-java-format-other.tmp'
        foreign.write_bytes(b'owned by another process')
        response = self.response()
        self.urlopen.side_effect = [response]
        create = self.formatter.tempfile.NamedTemporaryFile
        replace = self.formatter.os.replace
        streams = []

        def temporary_file(*arguments, **options):
            stream = create(*arguments, **options)
            streams.append(stream)
            return stream

        def publish(source, destination):
            temporary = Path(source)
            self.assertEqual(1, len(streams))
            self.assertTrue(streams[0].closed)
            self.assertTrue(response.closed)
            self.assertEqual(self.jar.parent, temporary.parent)
            self.assertNotEqual('.jar', temporary.suffix)
            self.assertNotEqual(foreign, temporary)
            self.assertEqual(self.DATA, temporary.read_bytes())
            self.assertEqual(self.jar, Path(destination))
            replace(source, destination)

        with patch.object(self.formatter.tempfile, 'NamedTemporaryFile', side_effect=temporary_file), \
                patch.object(self.formatter.os, 'replace', side_effect=publish) as publication:
            self.assertEqual(self.jar, self.formatter.ensure_formatter())
        publication.assert_called_once()
        self.assertEqual(self.DATA, self.jar.read_bytes())
        self.assertEqual(b'owned by another process', foreign.read_bytes())
        self.assertCountEqual([foreign, self.jar], self.jar.parent.iterdir())

    def test_partial_temporary_write_failure_cleans_only_the_owned_file(self):
        self.jar.parent.mkdir()
        foreign = self.jar.parent / '.google-java-format-other.tmp'
        foreign.write_bytes(b'keep this temporary file')
        response = self.response()
        self.urlopen.side_effect = [response]
        create = self.formatter.tempfile.NamedTemporaryFile
        streams = []

        def temporary_file(*arguments, **options):
            stream = create(*arguments, **options)
            streams.append(stream)
            return FailingWrite(stream)

        with patch.object(self.formatter.tempfile, 'NamedTemporaryFile', side_effect=temporary_file), \
                patch.object(self.formatter.os, 'replace') as publication:
            with self.assertRaisesRegex(OSError, 'temporary write failed'):
                self.formatter.ensure_formatter()
        self.assertEqual(1, len(streams))
        self.assertTrue(streams[0].closed)
        self.assertTrue(response.closed)
        publication.assert_not_called()
        self.assertFalse(self.jar.exists())
        self.assertEqual([foreign], list(self.jar.parent.iterdir()))
        self.assertEqual(b'keep this temporary file', foreign.read_bytes())
        self.urlopen.assert_called_once()
        self.sleep.assert_not_called()

    def test_replace_failure_leaves_no_partial_final_or_owned_temporary_file(self):
        response = self.response()
        self.urlopen.side_effect = [response]
        with patch.object(self.formatter.os, 'replace', side_effect=OSError('replace failed')):
            with self.assertRaisesRegex(OSError, 'replace failed'):
                self.formatter.ensure_formatter()
        self.assertTrue(response.closed)
        self.urlopen.assert_called_once()
        self.sleep.assert_not_called()
        self.assert_no_download_files()

    def test_published_file_is_hashed_again_before_return(self):
        response = self.response()
        self.urlopen.side_effect = [response]
        replace = self.formatter.os.replace

        def publish_changed_bytes(source, destination):
            replace(source, destination)
            Path(destination).write_bytes(b'changed after publication')

        with patch.object(self.formatter.os, 'replace', side_effect=publish_changed_bytes):
            with self.assertRaises(RuntimeError):
                self.formatter.ensure_formatter()
        self.assertEqual(b'changed after publication', self.jar.read_bytes())
        self.assertEqual([self.jar], list(self.jar.parent.iterdir()))
        self.assertTrue(response.closed)
        self.urlopen.assert_called_once()
        self.sleep.assert_not_called()

    def test_check_and_apply_preserve_flags_and_sorted_source_selection(self):
        self.cache()
        included = ('processors/src/test/Z.java', 'demo/src/main/B.java',
                    'processors/src/main/A.java', 'processors/src/milo1/M.java')
        for name in (*included, 'other/Skip.java', 'processors/target/Generated.java',
                     'demo/src/main/notes.txt'):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('synthetic source')
        sources = sorted(str(self.root / name) for name in included)
        for arguments, flags in (((), ['--replace']),
                                 (('--check',), ['--dry-run', '--set-exit-if-changed'])):
            with self.subTest(arguments=arguments):
                self.java.reset_mock()
                self.run_main(*arguments)
                self.java.assert_called_once_with(
                    ['java', '-jar', str(self.jar), '--aosp', *flags, *sources], check=True)
        self.urlopen.assert_not_called()
        self.sleep.assert_not_called()

    def test_java_failures_propagate_without_download_retry(self):
        self.cache()
        for failure in (subprocess.CalledProcessError(1, ['java']),
                        FileNotFoundError('java unavailable')):
            with self.subTest(failure=type(failure).__name__):
                self.java.reset_mock()
                self.java.side_effect = failure
                with self.assertRaises(type(failure)) as raised:
                    self.run_main('--check')
                self.assertIs(failure, raised.exception)
                self.java.assert_called_once()
                self.urlopen.assert_not_called()
                self.sleep.assert_not_called()

    def test_download_only_prints_verified_relative_path_and_keeps_retry_logs_on_stderr(self):
        error, body = self.http_error(429)
        response = self.response()
        self.urlopen.side_effect = [error, response]
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.run_main('--download-only')
        expected = self.jar.relative_to(self.root).as_posix() + '\n'
        self.assertEqual(expected, stdout.getvalue())
        self.assertTrue(self.stderr.getvalue())
        self.assertEqual(self.DATA, self.jar.read_bytes())
        self.assertTrue(body.closed)
        self.assertTrue(response.closed)
        self.java.assert_not_called()
        self.sleep.assert_called_once_with(10)
        self.urlopen.reset_mock(side_effect=True)
        self.sleep.reset_mock()
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.run_main('--download-only')
        self.assertEqual(expected, stdout.getvalue())
        self.urlopen.assert_not_called()
        self.sleep.assert_not_called()
        self.java.assert_not_called()

    def test_download_only_and_check_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit) as raised:
            self.run_main('--download-only', '--check')
        self.assertEqual(2, raised.exception.code)
        self.urlopen.assert_not_called()
        self.java.assert_not_called()
        self.sleep.assert_not_called()

    def test_download_only_setup_failure_has_nonzero_status_and_clean_stdout(self):
        error, body = self.http_error(404)
        self.urlopen.side_effect = error
        stdout = io.StringIO()
        with redirect_stdout(stdout), self.assertRaises(SystemExit) as raised:
            self.run_main('--download-only')
        self.assertEqual(1, raised.exception.code)
        self.assertEqual('', stdout.getvalue())
        self.assertTrue(self.stderr.getvalue())
        self.assertTrue(body.closed)
        self.java.assert_not_called()
        self.sleep.assert_not_called()
        self.assert_no_download_files()


class ReleaseFormattingTest(unittest.TestCase):
    def setUp(self):
        self.release = load('release')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.source = root / 'export'
        self.sources = ('processors/src/main/Z.java', 'processors/src/test/A.java',
                        'demo/src/main/B.java')
        for name in (*self.sources, 'other/Skip.java'):
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('synthetic Java source')
        patcher = patch.object(self.release, 'ROOT', root / 'checkout')
        patcher.start()
        self.addCleanup(patcher.stop)
        self.jar = '.tools/google-java-format-exported.jar'
        self.docker = Mock()
        self.log = io.StringIO()

    def test_shared_bootstrap_and_java21_check_use_the_exported_source(self):
        with patch.object(self.release.subprocess, 'check_output', return_value=self.jar + '\n') as bootstrap:
            self.release.check_formatting(self.source, self.docker, self.log)
        bootstrap.assert_called_once_with(
            ['python3', 'scripts/format-java.py', '--download-only'], cwd=self.source, text=True)
        self.docker.assert_called_once()
        java, arguments, log = self.docker.call_args.args
        self.assertEqual('21', java)
        self.assertEqual(['java', '-jar', self.jar, '--aosp', '--dry-run', '--set-exit-if-changed'],
                         arguments[:6])
        self.assertCountEqual(self.sources, arguments[6:])
        self.assertIs(self.log, log)

    def test_failed_bootstrap_prevents_the_docker_check(self):
        failure = subprocess.CalledProcessError(1, ['python3', 'scripts/format-java.py'])
        with patch.object(self.release.subprocess, 'check_output', side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                self.release.check_formatting(self.source, self.docker, self.log)
        self.assertIs(failure, raised.exception)
        self.docker.assert_not_called()

    def test_docker_formatter_failure_propagates(self):
        failure = subprocess.CalledProcessError(1, ['java', '-jar', self.jar])
        self.docker.side_effect = failure
        with patch.object(self.release.subprocess, 'check_output', return_value=self.jar + '\n') as bootstrap:
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                self.release.check_formatting(self.source, self.docker, self.log)
        self.assertIs(failure, raised.exception)
        bootstrap.assert_called_once()
        self.docker.assert_called_once()
