<<<<<<< HEAD
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    pass
=======
import orthauth as oa
auth = oa.configure_here('auth-config.py', __name__)
>>>>>>> master
