"""
Stream classes for NetSuite tap
"""

from .base import BaseStream
from .dimension import DimensionStream
from .gl_detail import GLDetailStream

__all__ = ['BaseStream', 'DimensionStream', 'GLDetailStream']
