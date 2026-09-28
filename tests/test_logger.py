import io
import logging
import unittest
from unittest.mock import patch

from src.logger import create_logger


class LoggerTests(unittest.TestCase):
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
