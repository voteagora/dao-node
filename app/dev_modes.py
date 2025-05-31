

import os

from dotenv import load_dotenv

load_dotenv()

CAPTURE_CLIENT_OUTPUTS_TO_DISK = bool(os.getenv('CAPTURE_CLIENT_OUTPUTS_TO_DISK', False))
CAPTURE_WS_CLIENT_OUTPUTS = bool(os.getenv('CAPTURE_WS_CLIENT_OUTPUTS', True))
PROFILE_ARCHIVE_CLIENT = bool(os.getenv('PROFILE_ARCHIVE_CLIENT', False))