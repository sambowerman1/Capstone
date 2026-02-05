"""
Timing utilities for measuring scraper performance.
"""

import time
import logging
from typing import Optional, Dict, List
from contextlib import contextmanager


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)


class Timer:
    """Context manager for timing code blocks."""
    
    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        """
        Initialize a Timer.
        
        Args:
            name: Name/description of the timed operation
            logger: Logger instance (uses module logger if not provided)
        """
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.start = 0.0
        self.elapsed = 0.0
    
    def __enter__(self):
        self.start = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.start
        self.logger.info(f"[TIMING] {self.name}: {self.elapsed:.2f}s")


class TimingStats:
    """Collect and report timing statistics across multiple operations."""
    
    def __init__(self, name: str = "Scraper"):
        """
        Initialize timing stats collector.
        
        Args:
            name: Name for this stats collection
        """
        self.name = name
        self.timings: Dict[str, List[float]] = {}
        self.logger = logging.getLogger(__name__)
        self.total_start: Optional[float] = None
        self.total_elapsed: float = 0.0
    
    def start_total(self):
        """Start the total timer."""
        self.total_start = time.perf_counter()
    
    def stop_total(self):
        """Stop the total timer."""
        if self.total_start is not None:
            self.total_elapsed = time.perf_counter() - self.total_start
    
    @contextmanager
    def time(self, operation: str):
        """
        Context manager to time an operation.
        
        Args:
            operation: Name of the operation being timed
            
        Yields:
            Timer instance with elapsed time after completion
        """
        timer = Timer(operation, self.logger)
        with timer:
            yield timer
        
        # Record the timing
        if operation not in self.timings:
            self.timings[operation] = []
        self.timings[operation].append(timer.elapsed)
    
    def record(self, operation: str, elapsed: float):
        """
        Manually record a timing.
        
        Args:
            operation: Name of the operation
            elapsed: Elapsed time in seconds
        """
        if operation not in self.timings:
            self.timings[operation] = []
        self.timings[operation].append(elapsed)
        self.logger.info(f"[TIMING] {operation}: {elapsed:.2f}s")
    
    def get_summary(self) -> str:
        """
        Generate a summary report of all timings.
        
        Returns:
            Formatted summary string
        """
        lines = [
            "",
            "=" * 60,
            f"TIMING SUMMARY: {self.name}",
            "=" * 60,
        ]
        
        if self.total_elapsed > 0:
            lines.append(f"Total Time: {self.total_elapsed:.2f}s ({self.total_elapsed/60:.1f} min)")
            lines.append("-" * 40)
        
        for operation, times in sorted(self.timings.items()):
            count = len(times)
            total = sum(times)
            avg = total / count if count > 0 else 0
            
            if count == 1:
                lines.append(f"  {operation}: {total:.2f}s")
            else:
                lines.append(f"  {operation}: {total:.2f}s total ({count}x, avg {avg:.2f}s)")
        
        lines.append("=" * 60)
        return "\n".join(lines)
    
    def print_summary(self):
        """Print the timing summary to the logger."""
        self.logger.info(self.get_summary())


# Global stats instance for easy access
_global_stats: Optional[TimingStats] = None


def get_global_stats() -> TimingStats:
    """Get or create the global timing stats instance."""
    global _global_stats
    if _global_stats is None:
        _global_stats = TimingStats()
    return _global_stats


def reset_global_stats():
    """Reset the global timing stats."""
    global _global_stats
    _global_stats = None

