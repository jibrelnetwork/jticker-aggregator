from jticker_aggregator.checker import RangeCollection, Range


THRESHOLD = 60


def test_complex():
    collection = RangeCollection(threshold=THRESHOLD)

    collection.add_value(0)
    collection.add_value(THRESHOLD)
    collection.add_value(THRESHOLD * 4)
    collection.add_value(THRESHOLD * 2)
    collection.add_value(THRESHOLD * 5)

    assert len(collection.ranges) == 2

    assert collection.ranges[0].start == -THRESHOLD
    assert collection.ranges[0].end == THRESHOLD * 2

    assert collection.ranges[1].start == THRESHOLD * (4 - 1)
    assert collection.ranges[1].end == THRESHOLD * 5

    collection.add_value(THRESHOLD * 10)

    assert len(collection.ranges) == 3

    assert collection.ranges[2].start == THRESHOLD * (10 - 1)
    assert collection.ranges[2].end == THRESHOLD * (10 + 1)

    # test merge
    collection.add_value(THRESHOLD * 3)

    assert len(collection.ranges) == 2


def test_insert_before():
    collection = RangeCollection(threshold=THRESHOLD)
    collection.add_value(THRESHOLD * 4)
    collection.add_value(THRESHOLD)

    assert len(collection.ranges) == 2


def test_merge_after():
    collection = RangeCollection(threshold=THRESHOLD)
    collection.add_value(THRESHOLD * 3)
    collection.add_value(THRESHOLD)
    collection.add_value(THRESHOLD * 2)

    assert len(collection.ranges) == 1


def test_range_repr():
    repr(Range(1, 2))
