"""YuktiSetu coding-profile scraper.

Reads the public coding-platform profiles students submit, and writes one
normalised snapshot per student per platform into the shared yuktisetu database
for the composite scoring engine to read.

This service is the only writer of coding_profile_snapshots.
"""

__version__ = "0.1.0"
