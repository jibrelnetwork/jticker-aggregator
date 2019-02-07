import logging
from datetime import datetime
from typing import List


class Range:
    start: int
    end: int

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __repr__(self):
        start = datetime.fromtimestamp(self.start)
        end = datetime.fromtimestamp(self.end)
        return f'<Range {start.isoformat()} - {end.isoformat()}>'


class RangeCollection:
    threshold: int
    ranges: List[Range]

    def __init__(self, threshold=60):
        self.threshold = threshold
        self.ranges = []

    def add_value(self, value):
        for i, r in enumerate(self.ranges):
            logging.debug("Range %i: %s", i, r)
            if r.end + self.threshold < value:
                # search next ranges
                logging.debug("Search next range")
                continue
            if r.start - self.threshold > value:
                # insert range before this interval
                new_r = Range(value, value)
                logging.debug("Insert new range at %i %s", i, new_r)
                self.ranges.insert(
                    i,
                    new_r
                )
                break
            # the value in range, check if we need extend edges
            if r.start > value:
                logging.debug("Start edge moved to new value %i -> %i",
                              r.start, value)
                r.start = value
                if i > 0 and self.ranges[i - 1].end >= r.start:
                    self.merge_ranges(i - 1, i)
            elif r.end < value:
                logging.debug("End edge moved to new value %i <- %i",
                              value, r.end)
                r.end = value
                if len(self.ranges) > i + 1 and self.ranges[i + 1].start <= r.end:
                    self.merge_ranges(i, i + 1)
            break
        else:
            self.ranges.append(Range(value - self.threshold, value + self.threshold))

    def merge_ranges(self, i1: int, i2: int):
        """Merge ranges with indexes i1 and i2.

        :param i1: first range index
        :param i2: second range index
        :return:
        """
        assert i2 == i1 + 1
        new_r = Range(self.ranges[i1].start, self.ranges[i2].end)
        del self.ranges[i2]
        del self.ranges[i1]
        self.ranges.insert(i1, new_r)
