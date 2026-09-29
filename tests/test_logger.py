import io
import logging
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.logger import create_logger, log_execution_metrics


class LoggerTests(unittest.TestCase):
    def test_execution_metrics_use_stderr_and_normalize_peak_memory(self):
        root = logging.getLogger()
        logger = logging.getLogger('alphapool-portfolio')
        self.addCleanup(logger.setLevel, logger.level)
        for platform, peak in (('linux', 2048), ('darwin', 2097152)):
            with self.subTest(platform=platform), patch.object(root, 'handlers', []), \
                    patch('sys.stderr', new_callable=io.StringIO) as stderr, \
                    patch('sys.stdout', new_callable=io.StringIO) as stdout, \
                    patch('src.logger.sys.platform', platform), \
                    patch('src.logger.perf_counter', side_effect=[10, 12.5]), \
                    patch('src.logger.resource.getrusage', return_value=SimpleNamespace(ru_maxrss=peak)):
                with log_execution_metrics(create_logger('info')):
                    pass
                self.assertIn('status=completed elapsed_seconds=2.500 peak_rss_mib=2.0', stderr.getvalue())
                self.assertEqual(stdout.getvalue(), '')
                root.handlers[0].close()

    def test_execution_failure_is_logged_and_propagated(self):
        logger = Mock()
        failure = RuntimeError('failed')
        with self.assertRaises(RuntimeError) as caught:
            with log_execution_metrics(logger):
                raise failure
        self.assertIs(caught.exception, failure)
        self.assertEqual(logger.info.call_count, 1)
        self.assertEqual(logger.info.call_args.args[1], 'failed')

    def test_repeated_setup_does_not_duplicate_logs_or_enable_external_info(self):
        output = io.StringIO()
        root = logging.getLogger()
        logger = logging.getLogger('alphapool-portfolio')
        self.addCleanup(logger.setLevel, logger.level)
        with patch.object(root, 'handlers', []), patch.object(root, 'level', logging.WARNING), \
                patch('sys.stderr', output):
            create_logger('debug')
            create_logger('debug').debug('application message')
            logging.getLogger('test.external').info('external message')
            self.assertEqual(len(root.handlers), 1)
            self.assertEqual(output.getvalue().count('application message'), 1)
            self.assertNotIn('external message', output.getvalue())
            root.handlers[0].close()

    def test_existing_root_configuration_is_preserved(self):
        root = logging.getLogger()
        handler = logging.NullHandler()
        logger = logging.getLogger('alphapool-portfolio')
        self.addCleanup(logger.setLevel, logger.level)
        with patch.object(root, 'handlers', [handler]), patch.object(root, 'level', logging.ERROR):
            result = create_logger(None)
            self.assertEqual(result.level, logging.INFO)
            self.assertEqual(root.level, logging.ERROR)
            self.assertEqual(root.handlers, [handler])


if __name__ == '__main__':
    unittest.main()
